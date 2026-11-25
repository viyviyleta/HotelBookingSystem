using Xunit;

// Общий ресурс сервера (DataStorage) — статический, поэтому тесты на синхронизацию
// должны запускаться последовательно: иначе параллельные наборы тестов мешают друг другу.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
