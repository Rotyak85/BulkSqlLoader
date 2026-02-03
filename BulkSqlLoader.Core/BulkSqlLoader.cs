using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Loaders;

public sealed class BulkSqlLoader
{
    private IDbConnection _connection;

    /// <summary>
    /// Readonly per ottimizzazione JIT.
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    /// Maximum limit of parameters into command-text: some examples:
    /// Ingres : 1024
    /// Microsoft Access : 768
    /// Oracle : 32767
    /// PostgresSQL : 32767
    /// SQLite : 999
    /// SQL Server : 2100 (depending on the version)
    /// Sybase ASE : 2000
    /// Taken from https://www.jooq.org/doc/3.12/manual/sql-building/dsl-context/custom-settings/settings-inline-threshold/
    /// </summary>
    public int ParamsBatchLimit { get; }

    /// <summary>
    /// If true, it raises an exception and aborts the transaction.
    /// </summary>
    public bool ThrowException { get; }

    /// <summary>
    /// Constructor of instance.
    /// </summary>
    /// <param name="connection">Connection istance.</param>
    /// <param name="logger">Set log istance (Nlog, Serilog.. etc).</param>
    /// <param name="throwException">If true raises exception and stops the transactions.</param>
    /// <param name="paramsBatchLimit">Server parameters limit for a single query</param>
    public BulkSqlLoader(IDbConnection connection, ILogger logger = null, bool throwException = false, int paramsBatchLimit = 2100)
    {
        _connection = connection;
        _logger = logger;

        ThrowException = throwException;
        ParamsBatchLimit = paramsBatchLimit;
    }

    /// <summary>
    /// Starts the connection.
    /// </summary>
    /// <param name="retry">Number of attempts.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)] // Inline to avoid call overhead
    private IDbConnection Connect(int retry = 1)
    {
        // Check if already open - avoid accessing State on disposed connection
        try
        {
            if (_connection is not null && _connection.State == ConnectionState.Open)
            {
                return _connection;
            }
        }
        catch (ObjectDisposedException)
        {
            // Connection was disposed, will need to reopen
        }

        // Goto removed from previous code, using loop for better branch prediction
        for (var attempt = 1; attempt <= retry; attempt++)
        {
            try
            {
                if (_connection is not null && _connection.State == ConnectionState.Closed)
                {
                    _connection.Open();
                }
                break; // Success, exit loop
            }
            catch (Exception ex)
            {
                if (_logger is not null)
                {
                    _logger.LogError(ex, "Connection attempt number {Attempt} failed", attempt);
                }

                if (attempt == retry)
                {
                    // DON'T dispose - let the caller handle it
                    throw;
                }
            }
        }

        return _connection;
    }

    /// <summary>
    /// Run generic query.
    /// </summary>
    /// <param name="query">Query to execute.</param>
    /// <returns>Results list.</returns>
    public IEnumerable<object[]> ExecuteQuery(string query)
    {
        List<object[]> results = [];

        _connection = Connect();

        IDbCommand command = null;
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            using var reader = command.ExecuteReader();
            var fieldCount = reader.FieldCount;
            
            while (reader.Read())
            {
                // Allocazione precisa per evitare resize della List interna
                var row = new object[fieldCount];

                // LINQ rimosso dal precedente codice, Loop manuale più veloce
                for (var i = 0; i < fieldCount; ++i)
                {
                    row[i] = reader[i];
                }

                results.Add(row);
            }
        }
        catch (Exception ex)
        {
            if (_logger is not null)
            {
                _logger.LogError(ex, "ExecuteQuery failed");
            }

            if (ThrowException)
            {
                throw;
            }
        }
        finally
        {
            command?.Dispose();
            _connection?.Close();
        }

        return results;
    }

    /// <summary>
    /// Count with span to avoid allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountParameters(ReadOnlySpan<char> query)
    {
        var count = 0;
        
        //ReadOnlySpan<char> to get zero-allocation string processing.
        foreach (var c in query)
        {
            if (c == '@')
                count++;
        }
        return count;
    }

    /// <summary>
    /// Performs a single parameterized non query.
    /// </summary>
    /// <param name="nonQuery">Non query to execute, example: "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)".</param>
    /// <param name="parameters">Parameters to replace, example: [ 1, "hello" ].</param>
    /// <param name="isolationLevel">Set the transaction lock.</param>
    /// <returns>Number of affected rows</returns>
    public int ExecuteNonQuery(string nonQuery, object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {
        var result = -1;

        _connection = Connect();

        IDbCommand command = null;
        IDbTransaction transaction = null;
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = nonQuery;

            transaction = _connection.BeginTransaction(isolationLevel);
            command.Transaction = transaction;

            var numberOfParameterQuery = CountParameters(nonQuery.AsSpan());

            // Pre-allocation to avoid multiple resizes
            for (var i = 0; i < numberOfParameterQuery; ++i)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = $"@p{i}";
                parameter.Value = parameters[i];
                command.Parameters.Add(parameter);
            }

            result = command.ExecuteNonQuery();
            transaction.Commit();
        }
        catch (Exception ex)
        {
            try
            {
                transaction?.Rollback();
            }
            catch (Exception ex1)
            {
                if (_logger is not null)
                {
                    _logger.LogError(ex1, "Rollback failed");
                }
            }

            if (_logger is not null)
            {
                _logger.LogError(ex, "ExecuteNonQuery failed");
            }

            if (ThrowException)
                throw;
        }
        finally
        {
            transaction?.Dispose();
            command?.Dispose();

            _connection?.Close();
        }

        return result;
    }

    /// <summary>
    /// Create a command with multiple non-queries and their parameters.
    /// </summary>
    /// <param name="nonQueries">Non queries to execute, example: [ "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)", "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p2, @p3)".</param>
    /// <param name="parameters">Parameters to replace, example: [ 1, "HELLO", 2, "WORLD" ].</param>
    /// <param name="isolationLevel">set the transaction lock.</param>
    public void ExecuteNonQueries(string[] nonQueries, object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {
        _connection = Connect();

        IDbCommand command = null;
        IDbTransaction transaction = null;

        var paramsCounter = 0;
        var queryIterator = 0;

        try
        {
            while (queryIterator < nonQueries.Length)
            {
                command = _connection.CreateCommand();

                // Pre-allocate StringBuilder with reasonable capacity to avoid resize
                // Estimate: average 100 chars per query * average batch size
                var queryBuilder = new StringBuilder(capacity: 1024);

                var nQueriesInBatch = 0;
                var totBatchParams = 0;
                
                for (var j = queryIterator; j < nonQueries.Length; ++j)
                {
                    var nonQuery = nonQueries[j];

                    var nParams = CountParameters(nonQuery.AsSpan());

                    if (totBatchParams + nParams < ParamsBatchLimit)
                    {
                        queryBuilder.Append(nonQuery).Append(';');

                        for (var i = 0; i < nParams; ++i)
                        {
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = $"@p{paramsCounter + i}"; 
                            parameter.Value = parameters[paramsCounter + i];
                            command.Parameters.Add(parameter);
                        }

                        paramsCounter += nParams;
                        totBatchParams += nParams;
                        ++nQueriesInBatch;
                    }
                    else
                    {
                        break;
                    }
                }

                queryIterator += nQueriesInBatch;

                transaction = _connection.BeginTransaction(isolationLevel);

                command.CommandText = queryBuilder.ToString();
                command.Transaction = transaction;

                command.ExecuteNonQuery();

                transaction.Commit();
            }
        }
        catch (Exception ex)
        {
            try
            {
                transaction?.Rollback();
            }
            catch (Exception ex1)
            {
                if (_logger is not null)
                {
                    _logger.LogError(ex1, "Rollback failed in ExecuteNonQueries");
                }
            }

            if (_logger is not null)
            {
                _logger.LogError(ex, "ExecuteNonQueries failed");
            }

            if (ThrowException)
            {
                throw;
            }
        }
        finally
        {
            transaction?.Dispose();
            command?.Dispose();

            _connection?.Close();
        }
    }

    /// <summary>
    /// Gets the name of the columns of a specific table.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <returns>List of names.</returns>
    public IEnumerable<string> GetColumnsName(string tableName)
    {
        // Collection expression iniziale vuota
        List<string> results = [];

        var query = $"SELECT * FROM {tableName}";

        _connection = Connect();

        IDbCommand command = null;

        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            using var reader = command.ExecuteReader();

            var fieldCount = reader.FieldCount;
            
            results = new List<string>(fieldCount);

            for (var i = 0; i < fieldCount; ++i)
            {
                results.Add(reader.GetName(i));
            }
        }
        catch (Exception ex)
        {
            if (_logger is not null)
            {
                _logger.LogError(ex, "GetColumnsName failed for table {TableName}", tableName);
            }

            if (ThrowException)
                throw;
        }
        finally
        {
            command?.Dispose();

            _connection?.Close();
        }

        return results;
    }

    /// <summary>
    /// Run Generic query, map the results in a specific entities collection.
    /// </summary>
    /// <typeparam name="T">Class that map columns, the name of the columns is case-sensitive.</typeparam>
    /// <param name="query">Query to execute.</param>
    /// <returns>Results list of T objects.</returns>
    public IEnumerable<T> ExecuteQuery<T>(string query) where T : class, new()
    {
        List<T> results = [];

        _connection = Connect();

        IDbCommand command = null;

        TypeAnalyzer<T> typeAnalyzer = new();
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                T obj = new();
                
                var fieldCount = reader.FieldCount;
                for (var i = 0; i < fieldCount; ++i)
                {
                    var key = reader.GetName(i);
                    var value = reader[i];

                    // ContainsKey + indexer removed from previous code, TryGetValue is more efficient
                    if (typeAnalyzer.PropertiesIndex.TryGetValue(key, out var propertyInfo))
                    {
                        propertyInfo.SetValue(obj, value is DBNull ? null : value);
                    }
                }

                results.Add(obj);
            }
        }
        catch (Exception ex)
        {
            if (_logger is not null)
            {
                _logger.LogError(ex, "ExecuteQuery<T> failed");
            }

            if (ThrowException)
                throw;
        }
        finally
        {
            command?.Dispose();

            _connection?.Close();
        }

        return results;
    }

    #region Async versions

    /// <summary>
    /// Starts the connection, async version.
    /// </summary>
    /// <param name="retry">Number of attempts.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<IDbConnection> ConnectAsync(int retry = 1)
    {
        // Check if already open - avoid accessing State on disposed connection
        try
        {
            if (_connection is not null && _connection.State == ConnectionState.Open)
            {
                return _connection;
            }
        }
        catch (ObjectDisposedException)
        {
            // Connection was disposed, will need to reopen
        }

        for (var attempt = 1; attempt <= retry; attempt++)
        {
            try
            {
                if (_connection is not null && _connection.State == ConnectionState.Closed)
                {
                    await Task.Run(() => _connection.Open())
                        .ConfigureAwait(false);
                }
                break; // Success, exit loop
            }
            catch (Exception ex)
            {
                if (_logger is not null)
                {
                    _logger.LogError(ex, "Connection async attempt number {Attempt} failed", attempt);
                }

                if (attempt == retry)
                {
                    throw;
                }
            }
        }

        return _connection;
    }

    /// <summary>
    /// Run generic query, async version.
    /// </summary>
    /// <param name="query">Query to execute.</param>
    /// <returns>Results list.</returns>
    public async Task<IEnumerable<object[]>> ExecuteQueryAsync(string query)
    {
        List<object[]> results = [];

        _connection = await ConnectAsync().ConfigureAwait(false);

        IDbCommand command = null;
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            var reader = await Task.Run(() => command.ExecuteReader())
                .ConfigureAwait(false);

            await Task.Run(() =>
            {
                try
                {
                    var fieldCount = reader.FieldCount;
                    while (reader.Read())
                    {
                        var row = new object[fieldCount];

                        for (var i = 0; i < fieldCount; ++i)
                        {
                            row[i] = reader[i];
                        }

                        results.Add(row);
                    }
                }
                finally
                {
                    reader.Dispose();
                }
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (_logger is not null)
            {
                _logger.LogError(ex, "ExecuteQueryAsync failed");
            }

            if (ThrowException)
                throw;
        }
        finally
        {
            command?.Dispose();
            _connection?.Close();
        }

        return results;
    }

    /// <summary>
    /// Performs a single parameterized non query, async version.
    /// </summary>
    /// <param name="nonQuery">Non query to execute, example: "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)".</param>
    /// <param name="parameters">Parameters to replace, example: [ 1, "hello" ].</param>
    /// <param name="isolationLevel">Set the transaction lock.</param>
    /// <returns>Number of affected rows.</returns>
    public async Task<int> ExecuteNonQueryAsync(string nonQuery, object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {
        var result = -1;

        _connection = await ConnectAsync().ConfigureAwait(false);

        IDbCommand command = null;
        IDbTransaction transaction = null;
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = nonQuery;

            transaction = await Task.Run(() => _connection.BeginTransaction(isolationLevel)).ConfigureAwait(false);
            command.Transaction = transaction;

            var numberOfParameterQuery = CountParameters(nonQuery.AsSpan());

            for (var i = 0; i < numberOfParameterQuery; ++i)
            {
                var parameter = command.CreateParameter();
                
                parameter.ParameterName = $"@p{i}";
                parameter.Value = parameters[i];
                command.Parameters.Add(parameter);
            }

            var cmd = command;
            var trans = transaction;
            
            result = await Task.Run(() =>
            {
                var r = cmd.ExecuteNonQuery();
                
                trans.Commit();
                return r;
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            try
            {
                if (transaction is not null)
                {
                    var trans = transaction;
                    await Task.Run(() => trans.Rollback())
                        .ConfigureAwait(false);
                }
            }
            catch (Exception ex1)
            {
                _logger?.LogError(ex1, "Rollback failed in ExecuteNonQueryAsync");
            }

            _logger?.LogError(ex, "ExecuteNonQueryAsync failed");

            if (ThrowException)
                throw;
        }
        finally
        {
            transaction?.Dispose();
            command?.Dispose();
            
            _connection?.Close();
        }

        return result;
    }

    /// <summary>
    /// Create a command with multiple non-queries and their parameters, async version.
    /// </summary>
    /// <param name="nonQueries">Non queries to execute, example: [ "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)", "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p2, @p3)".</param>
    /// <param name="parameters">Parameters to replace, example: [ 1, "HELLO", 2, "WORLD" ].</param>
    /// <param name="isolationLevel">Set the transaction lock.</param>
    public async Task ExecuteNonQueriesAsync(string[] nonQueries, object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {
        _connection = await ConnectAsync().ConfigureAwait(false);

        IDbCommand command = null;
        IDbTransaction transaction = null;

        var paramsCounter = 0;
        var queryIterator = 0;

        try
        {
            while (queryIterator < nonQueries.Length)
            {
                command = _connection.CreateCommand();

                var queryBuilder = new StringBuilder(capacity: 1024);

                var nQueriesInBatch = 0;
                var totBatchParams = 0;
                
                for (var j = queryIterator; j < nonQueries.Length; ++j)
                {
                    var nonQuery = nonQueries[j];

                    var nParams = CountParameters(nonQuery.AsSpan());

                    if (totBatchParams + nParams < ParamsBatchLimit)
                    {
                        queryBuilder.Append(nonQuery).Append(';');

                        for (var i = 0; i < nParams; ++i)
                        {
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = $"@p{paramsCounter + i}";
                            parameter.Value = parameters[paramsCounter + i];
                            command.Parameters.Add(parameter);
                        }

                        paramsCounter += nParams;
                        totBatchParams += nParams;
                        ++nQueriesInBatch;
                    }
                    else
                    {
                        break;
                    }
                }

                queryIterator += nQueriesInBatch;

                transaction = await Task.Run(() => _connection.BeginTransaction(isolationLevel)).ConfigureAwait(false);

                command.CommandText = queryBuilder.ToString();
                command.Transaction = transaction;

                var cmd = command;
                var trans = transaction;
                
                await Task.Run(() =>
                {
                    cmd.ExecuteNonQuery();
                    trans.Commit();
                }).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            try
            {
                if (transaction is not null)
                {
                    var trans = transaction;
                    await Task.Run(() => trans.Rollback()).ConfigureAwait(false);
                }
            }
            catch (Exception ex1)
            {
                _logger?.LogError(ex1, "Rollback failed in ExecuteNonQueriesAsync");
            }

            _logger?.LogError(ex, "ExecuteNonQueriesAsync failed");

            if (ThrowException)
                throw;
        }
        finally
        {
            transaction?.Dispose();
            command?.Dispose();
            _connection?.Close();
        }
    }

    /// <summary>
    /// Gets the name of the columns of a specific table, async version.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <returns>List of names.</returns>
    public async Task<IEnumerable<string>> GetColumnsNameAsync(string tableName)
    {
        List<string> results = [];

        var query = $"SELECT * FROM {tableName}";

        _connection = await ConnectAsync().ConfigureAwait(false);

        IDbCommand command = null;

        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            var reader = await Task.Run(() => command.ExecuteReader()).ConfigureAwait(false);

            try
            {
                var fieldCount = reader.FieldCount;
                results = new List<string>(fieldCount);

                for (var i = 0; i < fieldCount; ++i)
                {
                    results.Add(reader.GetName(i));
                }
            }
            finally
            {
                reader.Dispose();
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "GetColumnsNameAsync failed for table {TableName}", tableName);

            if (ThrowException)
                throw;
        }
        finally
        {
            command?.Dispose();
            _connection?.Close();
        }

        return results;
    }

    /// <summary>
    /// Run Generic query, map the results in a specific entities collection, async version.
    /// </summary>
    /// <typeparam name="T">Class that map columns, the name of the columns is case-sensitive.</typeparam>
    /// <param name="query">Query to execute.</param>
    /// <returns>Results list of T objects.</returns>
    public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(string query) where T : class, new()
    {
        List<T> results = [];

        _connection = await ConnectAsync().ConfigureAwait(false);

        IDbCommand command = null;

        TypeAnalyzer<T> typeAnalyzer = new();
        try
        {
            command = _connection.CreateCommand();
            command.CommandText = query;

            var reader = await Task.Run(() => command.ExecuteReader()).ConfigureAwait(false);

            await Task.Run(() =>
            {
                try
                {
                    while (reader.Read())
                    {
                        T obj = new();
                        
                        var fieldCount = reader.FieldCount;
                        for (var i = 0; i < fieldCount; ++i)
                        {
                            var key = reader.GetName(i);
                            var value = reader[i];

                            if (typeAnalyzer.PropertiesIndex.TryGetValue(key, out var propertyInfo))
                            {
                                propertyInfo.SetValue(obj, value is DBNull ? null : value);
                            }
                        }

                        results.Add(obj);
                    }
                }
                finally
                {
                    reader.Dispose();
                }
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "ExecuteQueryAsync<T> failed");

            if (ThrowException)
                throw;
        }
        finally
        {
            command?.Dispose();
            _connection?.Close();
        }

        return results;
    }

    #endregion Async versions
}