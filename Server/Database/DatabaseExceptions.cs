namespace Server.Database;

public sealed class DatabaseConnectionException : Exception
{
    public DatabaseConnectionException(string message) : base(message) { }

    public DatabaseConnectionException(string message, Exception inner) : base(message, inner) { }
}


public sealed class InjectedOperationException : Exception
{
    public InjectedOperationException(string message) : base(message) { }
}

public sealed class CommitFailedException : Exception
{
    public CommitFailedException(string message, Exception inner) : base(message, inner) { }
}

public sealed class InstanceUnavailableException : Exception
{
    public InstanceUnavailableException(string message) : base(message) { }
}

public sealed class ReplicationFailedException : Exception
{
    public ReplicationFailedException(string message) : base(message) { }

    public ReplicationFailedException(string message, Exception inner) : base(message, inner) { }
}
