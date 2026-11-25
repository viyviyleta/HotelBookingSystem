namespace Client;

public sealed class ClientClock
{
    public static readonly ClientClock Unsynchronized = new(TimeSpan.Zero);

    private readonly Func<DateTime> _rawNow;

    public TimeSpan Skew { get; }

    public ClientClock(TimeSpan skew, Func<DateTime>? rawNow = null)
    {
        Skew = skew;
        _rawNow = rawNow ?? (() => DateTime.UtcNow);
    }

    public DateTime LocalNow() => LocalAt(_rawNow());

    public DateTime LocalAt(DateTime rawUtc) => rawUtc + Skew;

    public override string ToString() =>
        Skew == TimeSpan.Zero ? "client clock (no skew)" : $"client clock (skew {Skew.TotalSeconds:+0;-0} s)";
}

public enum CorrectionStrategy
{
    ///  Offset = ServerTime - ClientTime, задержка передачи игнорируется
    Naive,

    /// штамп сервера относится к середине интервала передачи (RTT/2)
    HalfRtt,

    ///усреднение оценок по времени приёма и отправки сервером
    Averaged
}


/// Результат одного обмена TIME_REQUEST - TIME_RESPONSE: замеренные клиентом времена
/// отправки/приёма и штампы сервера, из которых считаются поправки.
public sealed record TimeExchange(
    DateTime ClientSendRaw,
    DateTime ClientReceiveRaw,
    DateTime ServerReceivedAt,
    DateTime ServerAnsweredAt,
    ClientClock Clock)
{
    /// Round-Trip Time — полный путь запроса туда и обратно
    public TimeSpan Rtt => ClientReceiveRaw - ClientSendRaw;

    /// Упрощённая оценка задержки в одну сторону
    public TimeSpan EstimatedOneWayDelay => TimeSpan.FromTicks(Rtt.Ticks / 2);

    public DateTime LocalTimeAtSend => Clock.LocalAt(ClientSendRaw);

    public DateTime LocalTimeAtReceive => Clock.LocalAt(ClientReceiveRaw);

    /// Разница часов до синхронизации (локальное время клиента минус время сервера)
    public TimeSpan DifferenceBeforeSync => LocalTimeAtSend - ServerAnsweredAt;

    public TimeSpan Correction(CorrectionStrategy strategy) => strategy switch
    {
        CorrectionStrategy.Naive => ServerAnsweredAt - LocalTimeAtSend,

        CorrectionStrategy.HalfRtt => (ServerAnsweredAt - EstimatedOneWayDelay) - LocalTimeAtSend,

        CorrectionStrategy.Averaged => TimeSpan.FromTicks(
            ((ServerReceivedAt - LocalTimeAtSend).Ticks + (ServerAnsweredAt - LocalTimeAtReceive).Ticks) / 2),

        _ => throw new ArgumentOutOfRangeException(nameof(strategy))
    };

    public DateTime CorrectedNow(CorrectionStrategy strategy) => CorrectedAt(DateTime.UtcNow, strategy);

    public DateTime CorrectedAt(DateTime rawUtc, CorrectionStrategy strategy) => Clock.LocalAt(rawUtc) + Correction(strategy);

    public TimeSpan Error(CorrectionStrategy strategy, DateTime rawUtc, DateTime serverTimeNow) =>
        CorrectedAt(rawUtc, strategy) - serverTimeNow;
}
