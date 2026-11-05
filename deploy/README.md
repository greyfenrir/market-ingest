# Обновление сборщиков и миграция глубины

## Что выполняется автоматически

`./deploy/deploy.sh --git` забирает `main` на сервер, устанавливает зависимости,
создаёт отсутствующие таблицы и выполняет:

1. `deploy/migrations/book_depth.py --phase prepare`: добавляет `book_age_s`,
   сохраняя совместимость со старым сборщиком.
2. Перезапуск всех включённых или работающих `market-ingest@*.service` и
   проверку их активности. systemd дожидается завершения старых процессов;
   штатный SIGTERM позволяет сборщику отправить накопленный пакет.
3. `deploy/migrations/book_depth.py --phase finish`: переносит историю из
   `acc` с округлением, переводит глубины в `UInt64` и удаляет `acc` и
   `band_valid`. Новые сборщики продолжают писать во время переноса.

Подготовка, завершение и повторный деплой проверяются на настоящем ClickHouse
в PR. Ошибка любого шага останавливает деплой. Повторный запуск безопасен.
Перезапуск процесса не означает непрерывного получения сообщений биржи:
после него заново строится локальный стакан.

## Первое обновление с ручным завершением миграции

Для первого переключения можно проверить новые записи перед переносом истории.
Команды ниже используют SSH-алиас `scali`, заданный в `deploy.sh` по умолчанию.
При другом сервере используйте свой алиас и `VPS_ALIAS`.

1. Дождаться зелёной проверки PR, влить его и получить новый `main` локально:

   ```bash
   git switch main
   git pull --ff-only origin main
   ```

2. Развернуть код и переключить работающие сборщики, отложив перенос истории:

   ```bash
   ./deploy/deploy.sh --git --defer-migration-finish
   ```

   Скрипт сам подготовит таблицу и перезапустит сборщики. Отдельный `prepare`
   и копирование миграции через `scp` не нужны. При ошибке не переходить к
   завершению миграции: исправить причину и повторить деплой.

3. Проверить Binance и версию новых строк. При настройке пакета на 30 секунд
   дать ему время отправиться:

   ```bash
   ssh scali 'sudo systemctl is-active market-ingest@binance_um.service'
   ssh scali 'cat /etc/market-ingest/version'
   ssh scali clickhouse-client --multiquery <<'SQL'
   SELECT collector_version, state, count() AS rows, max(ingest_time) AS latest,
          max(depth_bid_usd_1) AS max_bid_depth_1pct, max(book_age_s) AS book_age_s
   FROM market.book_agg
   WHERE exchange = 'binance_um' AND ingest_time > now() - INTERVAL 2 MINUTE
   GROUP BY collector_version, state
   ORDER BY latest DESC;
   SQL
   ```

   Должны появиться строки новой версии из файла `version`, с ненулевой
   глубиной и состоянием `ok` после построения стакана. Для выяснения причин
   ошибок: `ssh scali sudo journalctl -u market-ingest@binance_um.service -n 50 --no-pager`.
   До переноса истории старые и новые основные поля имеют разный смысл;
   для анализа различайте их по `collector_version`.

4. После успешного переключения завершить миграцию:

   ```bash
   ssh scali 'python3 /opt/market-ingest/deploy/migrations/book_depth.py --phase finish --client clickhouse-client'
   ```

   Команда ждёт завершения переноса и преобразования типов. Она может
   выполняться дольше обычного DDL: время зависит от размера истории и диска.
   Исторические выборки по основным полям использовать после её успешного
   завершения: во время переноса разные части истории обновляются постепенно.
   При ошибке устранить причину и повторить ту же команду. Новые сборщики
   останавливать не требуется. После удаления старых столбцов старый сборщик
   возвращать нельзя: он пишет другую метрику под тем же именем. Историческая
   отдельная сумма внутри полного покрытия после `finish` удаляется.

## Включить Bybit

Деплой обновляет уже выбранные сборщики и не включает новую биржу автоматически.
Для первого запуска Bybit:

1. Добавить в `/etc/market-ingest/config.toml` секции `[bybit_linear]`,
   `[bybit_linear.stall_seconds]` и `[bybit_linear.depth]` из доставленного
   `/opt/market-ingest/config.example.toml`. Сохранить существующие настройки
   ClickHouse и Binance; выбрать нужные `focus_symbols`.

   ```bash
   ssh -t scali sudoedit /etc/market-ingest/config.toml
   ```

2. Включить отдельный процесс:

   ```bash
   ssh scali 'sudo systemctl enable --now market-ingest@bybit_linear.service'
   ssh scali 'sudo journalctl -u market-ingest@bybit_linear.service -n 50 --no-pager'
   ```

3. Повторить запрос новых строк, заменив `binance_um` на `bybit_linear`.

При последующих обновлениях достаточно `./deploy/deploy.sh --git`: подготовка,
перезапуск и завершение миграции выполняются автоматически.
