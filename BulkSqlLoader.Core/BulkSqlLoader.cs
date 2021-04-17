using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace BulkSqlLoader.Core
{
    public partial class BulkSqlLoader
    {
        /// <summary>
        /// Connection passed in constructor
        /// </summary>
        private IDbConnection _connection;

        /// <summary>
        /// ILogger instance for logging
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Maximum limit of parameters into command-text: some examples:
        /// Ingres : 1024
        /// Microsoft Access : 768
        /// Oracle : 32767
        /// PostgreSQL : 32767
        /// SQLite : 999
        /// SQL Server : 2100 (depending on the version)
        /// Sybase ASE : 2000
        /// Taken from https://www.jooq.org/doc/3.12/manual/sql-building/dsl-context/custom-settings/settings-inline-threshold/
        /// </summary>
        public int ParamsBatchLimit { get; set; }

        /// <summary>
        /// If true, it raises an exception and aborts the transaction
        /// </summary>
        public bool ThrowException { get; set; }

        /// <summary>
        /// Constructor of instance
        /// </summary>
        /// <param name="connection">Connection istance</param>
        /// <param name="logger">Set log istance (Nlog, Serilog.. etc)</param>
        /// <param name="throwException">If true raises exception and stops the transactions</param>
        public BulkSqlLoader(IDbConnection connection, ILogger logger = null, bool throwException = false, int paramsBatchLimit = 2100)
        {
            _connection = connection;
            _logger = logger;

            ThrowException = throwException;
            ParamsBatchLimit = paramsBatchLimit;
        }

        /// <summary>
        /// Starts the connection
        /// </summary>
        /// <param name="retry">Number of attempts</param>
        /// <returns>The open connection</returns>
        private IDbConnection Connect(int retry = 1)
        {
            _connection?.Close();

            var attempt = 1;
        START:
            try
            {
                _connection?.Open();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.Message);
                _logger?.LogError(ex.StackTrace);

                _logger?.LogError($"Connection attempt number {attempt} failed");

                ++attempt;

                if (attempt <= retry)
                    goto START;

                _connection?.Dispose();

                /*if connection goes wrong we can't do anything else: throws up the exception*/
                throw;
            }

            return _connection;
        }

        /// <summary>
        /// Run generic query
        /// </summary>
        /// <param name="query">Query to execute</param>
        /// <returns>Results list</returns>
        public IEnumerable<object[]> ExecuteQuery(string query)
        {
            List<object[]> result = new();

            _connection = Connect();

            IDbCommand command = null;
            try
            {
                command = _connection.CreateCommand();
                command.CommandText = query;

                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var row = new List<object>();

                    for (int i = 0; i < reader.FieldCount; ++i)
                    {
                        object value = reader[i];

                        object castedValue = Convert.ChangeType(value, value.GetType());

                        row.Add(castedValue);
                    }

                    result.Add(row.ToArray());
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.Message);
                _logger?.LogError(ex.StackTrace);

                if (ThrowException)
                    throw;
            }
            finally
            {
                command?.Dispose();

                _connection?.Close();
            }

            return result;
        }

        /// <summary>
        /// Performs a single parameterized non query
        /// </summary>
        /// <param name="nonQuery">Non query to execute, example: "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)"</param>
        /// <param name="parameters">Parameters to replace, example: [ 1, "hello" ]</param>
        /// <param name="isolationLevel">Set the transaction lock</param>
        /// <returns>Number of affected rows</returns>
        public int ExecuteNonQuery(string nonQuery, Object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            int result = -1;

            _connection = Connect();

            IDbCommand command = null;
            IDbTransaction transaction = null;
            try
            {
                command = _connection.CreateCommand();
                command.CommandText = nonQuery;

                transaction = _connection.BeginTransaction(isolationLevel);
                command.Transaction = transaction;

                _connection.CreateCommand();

                int numberOfParameterQuery = 0;
                for (int i = 0; i < nonQuery.Length; ++i)
                {
                    if (nonQuery[i] == '@')
                        ++numberOfParameterQuery;
                }

                for (int i = 0; i < numberOfParameterQuery; ++i)
                {
                    var parameter = command.CreateParameter();

                    parameter.ParameterName = "@p" + i;
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
                    _logger?.LogError(ex1.Message);
                    _logger?.LogError(ex1.StackTrace);

                    /*Rollback exception isn't throw out in favor of the higher exception*/
                }

                _logger?.LogError(ex.Message);
                _logger?.LogError(ex.StackTrace);

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
        /// Create a command with multiple non-queries and their parameters
        /// </summary>
        /// <param name="nonQueries">Non queries to execute, example: [ "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)", "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p2, @p3)" </param>
        /// <param name="parameters">Parameters to replace, example: [ 1, "HELLO", 2, "WORLD" ]</param>
        /// <param name="isolationLevel">set the transaction lock</param>
        public void ExecuteNonQueries(string[] nonQueries, Object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            _connection = Connect();

            IDbCommand command = null;
            IDbTransaction transaction = null;

            var queryBuilder = new StringBuilder();

            /*counts the progressive number of batch's parameters during iteration*/
            var paramsCounter = 0;

            /*contains the position of the first non-query that will end up in the next batch, every batch it increments of BatchSize*/
            var queryIterator = 0;

            /*contains the progressive number of batches already executed*/
            var batchExecuted = 0;
            try
            {
                while (queryIterator < nonQueries.Length)
                {
                    command = _connection.CreateCommand();

                    queryBuilder = new StringBuilder();

                    /*support variable to not change queryIterator during loop*/
                    var nQueriesInBatch = 0;

                    var totBatchParams = 0;
                    for (int j = queryIterator; j < nonQueries.Length; ++j)
                    {
                        var nonQuery = nonQueries[j];

                        var nParams = nonQuery.Count(c => c == '@');

                        if (totBatchParams + nParams < ParamsBatchLimit)
                        {
                            queryBuilder.Append(nonQuery)
                                .Append(';');

                            for (int i = 0; i < nParams; ++i)
                            {
                                var parameter = command.CreateParameter();

                                parameter.ParameterName = "@p" + (paramsCounter + i);
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

                    ++batchExecuted;

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
                    if (transaction != null)
                    {
                        transaction?.Rollback();
                    }
                }
                catch (Exception ex1)
                {
                    _logger?.LogError(ex1.Message);
                    _logger?.LogError(ex1.StackTrace);

                    /*if ThrowException it throw main exception "ex"*/
                }

                _logger?.LogError(ex.Message);
                _logger?.LogError(ex.StackTrace);

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
    }

    public partial class BulkSqlLoader
    {
        /// <summary>
        /// Starts the connection, async version
        /// </summary>
        /// <param name="retry">Number of attempts</param>
        /// <returns>The open connection</returns>
        private async Task<IDbConnection> ConnectAsync(int retry = 1)
        {
            _connection?.Close();

            var attempt = 1;
        START:
            try
            {
                await Task.Run(() => _connection?.Open())
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.Message);
                _logger?.LogError(ex.StackTrace);

                _logger?.LogError($"Connection attempt number {attempt} failed");

                ++attempt;

                if (attempt <= retry)
                    goto START;

                _connection?.Dispose();

                /*if connection goes wrong we can't do anything else: throws up the exception*/
                throw;
            }

            return _connection;
        }

        /// <summary>
        /// Run generic query, async version
        /// </summary>
        /// <param name="query">Query to execute</param>
        /// /// <returns>Results list</returns>
        public async Task<IEnumerable<object[]>> ExecuteQueryAsync(string query)
        {
            List<object[]> result = new();

            _connection = await ConnectAsync()
                .ConfigureAwait(false);

            IDbCommand command = null;
            try
            {
                command = _connection.CreateCommand();
                command.CommandText = query;

                using var reader = await Task.Run(() => command.ExecuteReader())
                    .ConfigureAwait(false);

                await Task.Run(() =>
                {
                    while (reader.Read())
                    {
                        var row = new List<object>();

                        for (int i = 0; i < reader.FieldCount; ++i)
                        {
                            object value = reader[i];

                            object castedValue = Convert.ChangeType(value, value.GetType());

                            row.Add(castedValue);
                        }

                        result.Add(row.ToArray());
                    }
                }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await Task.Run(() =>
                {
                    _logger?.LogError(ex.Message);
                    _logger?.LogError(ex.StackTrace);
                }).ConfigureAwait(false);

                if (ThrowException)
                    throw;
            }
            finally
            {
                if (command != null)
                {
                    await Task.Run(() => command.Dispose())
                        .ConfigureAwait(false);
                }

                if (_connection != null)
                {
                    await Task.Run(() => _connection.Close())
                        .ConfigureAwait(false);
                }
            }

            return result;
        }

        /// <summary>
        /// Performs a single parameterized non query, async version
        /// </summary>
        /// <param name="nonQuery">Non query to execute, example: "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)"</param>
        /// <param name="parameters">Parameters to replace, example: [ 1, "hello" ]</param>
        /// <param name="isolationLevel">Set the transaction lock</param>
        /// <returns>Number of affected rows</returns>
        public async Task<int> ExecuteNonQueryAsync(String nonQuery, Object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            int result = -1;

            _connection = await ConnectAsync()
                .ConfigureAwait(false);

            IDbCommand command = null;
            IDbTransaction transaction = null;
            try
            {
                command = _connection.CreateCommand();
                command.CommandText = nonQuery;

                transaction = await Task.Run(() => _connection.BeginTransaction(isolationLevel))
                    .ConfigureAwait(false);

                command.Transaction = transaction;

                _connection.CreateCommand();

                int numberOfParameterQuery = 0;
                for (int i = 0; i < nonQuery.Length; ++i)
                {
                    if (nonQuery[i] == '@')
                        ++numberOfParameterQuery;
                }

                for (int i = 0; i < numberOfParameterQuery; ++i)
                {
                    var parameter = command.CreateParameter();

                    parameter.ParameterName = "@p" + i;
                    parameter.Value = parameters[i];

                    command.Parameters.Add(parameter);
                }

                result = await Task.Run(() =>
                {
                    var r = command.ExecuteNonQuery();
                    transaction.Commit();

                    return r;
                }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                try
                {
                    if (transaction != null)
                    {
                        await Task.Run(() => transaction.Rollback())
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception ex1)
                {
                    await Task.Run(() =>
                    {
                        _logger?.LogError(ex1.Message);
                        _logger?.LogError(ex1.StackTrace);
                    }).ConfigureAwait(false);

                    /*if ThrowException it throw main exception "ex"*/
                }

                await Task.Run(() =>
                {
                    _logger?.LogError(ex.Message);
                    _logger?.LogError(ex.StackTrace);
                }).ConfigureAwait(false);

                if (ThrowException)
                    throw;
            }
            finally
            {
                if (transaction != null)
                {
                    await Task.Run(() => transaction.Dispose())
                        .ConfigureAwait(false);
                }

                if (command != null)
                {
                    await Task.Run(() => command.Dispose())
                        .ConfigureAwait(false);
                }

                if (_connection != null)
                {
                    await Task.Run(() => _connection.Close())
                        .ConfigureAwait(false);
                }
            }

            return result;
        }

        /// <summary>
        /// Create a command with multiple non-queries and their parameters, async version
        /// </summary>
        /// <param name="nonQueries">Non queries to execute, example: [ "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p0, @p1)", "INSERT INTO TABLE_NAME (ID, DESC) VALUES (@p2, @p3)" </param>
        /// <param name="parameters">Parameters to replace, example: [ 1, "HELLO", 2, "WORLD" ]</param>
        /// <param name="isolationLevel">Set the transaction lock</param>
        public async Task ExecuteNonQueriesAsync(string[] nonQueries, Object[] parameters = null, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            _connection = await ConnectAsync()
                .ConfigureAwait(false);

            IDbCommand command = null;
            IDbTransaction transaction = null;

            var queryBuilder = new StringBuilder();

            /*counts the progressive number of batch's parameters during iteration*/
            var paramsCounter = 0;

            /*contains the position of the first non-query that will end up in the next batch, every batch it increments of BatchSize*/
            var queryIterator = 0;

            /*contains the progressive number of batches already executed*/
            var batchExecuted = 0;
            try
            {
                while (queryIterator < nonQueries.Length)
                {
                    command = _connection.CreateCommand();

                    queryBuilder = new StringBuilder();

                    /*support variable to not change queryIterator during loop*/
                    var nQueriesInBatch = 0;

                    var totBatchParams = 0;
                    for (int j = queryIterator; j < nonQueries.Length; ++j)
                    {
                        var nonQuery = nonQueries[j];

                        var nParams = nonQuery.Count(c => c == '@');

                        if (totBatchParams + nParams < ParamsBatchLimit)
                        {
                            queryBuilder.Append(nonQuery)
                                .Append(';');

                            for (int i = 0; i < nParams; ++i)
                            {
                                var parameter = command.CreateParameter();

                                parameter.ParameterName = "@p" + (paramsCounter + i);
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

                    ++batchExecuted;

                    queryIterator += nQueriesInBatch;

                    transaction = await Task.Run(() => _connection.BeginTransaction(isolationLevel))
                        .ConfigureAwait(false);

                    command.CommandText = queryBuilder.ToString();
                    command.Transaction = transaction;

                    await Task.Run(() =>
                    {
                        command.ExecuteNonQuery();

                        transaction.Commit();
                    }).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                try
                {
                    if (transaction != null)
                    {
                        await Task.Run(() => transaction.Rollback())
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception ex1)
                {
                    await Task.Run(() =>
                    {
                        _logger?.LogError(ex1.Message);
                        _logger?.LogError(ex1.StackTrace);
                    }).ConfigureAwait(false);

                    /*if ThrowException it throw main exception "ex"*/
                }

                await Task.Run(() =>
                {
                    _logger?.LogError(ex.Message);
                    _logger?.LogError(ex.StackTrace);
                }).ConfigureAwait(false);

                if (ThrowException)
                    throw;
            }
            finally
            {
                if (transaction != null)
                {
                    await Task.Run(() => transaction.Dispose())
                        .ConfigureAwait(false);
                }

                if (command != null)
                {
                    await Task.Run(() => command.Dispose())
                        .ConfigureAwait(false);
                }

                if (_connection != null)
                {
                    await Task.Run(() => _connection.Close())
                        .ConfigureAwait(false);
                }
            }
        }
    }
}