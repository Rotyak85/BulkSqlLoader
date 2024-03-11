using Loaders.Test.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Loaders.Test
{
    public abstract class BulkSqlLoaderTest
    {
        protected string _engineName;

        protected readonly IConfigurationSection _connectionStrings;
        protected readonly IConfigurationSection _queries;
        protected readonly IConfigurationSection _envirnoment;

        protected BulkSqlLoader _bulkSqlLoader;

        protected BulkSqlLoaderTest()
        {
            IConfiguration builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .Build();

            _connectionStrings = builder
                .GetSection("configuration:connectionStrings");

            _queries = builder.GetSection("configuration:queries");
            _envirnoment = builder.GetSection("configuration:env");
        }

        protected void InitializeEnvironment()
        {
            var autoInit = Convert.ToBoolean(
                    _envirnoment.GetSection("auto-init")?.Value
                    ?? throw new Exception("appsetting.json must contains a select count query in the section configuration:connectionStrings"));

            var tab = _envirnoment.GetSection("table");

            if (!autoInit)
                return;

            try
            {
                var nonQuery = $"DROP TABLE {tab.Value}";

                _bulkSqlLoader.ExecuteNonQuery(nonQuery);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);

                /*if it does not exist, it goes further*/
                /*because "if exists" isn't in standard sql*/
            }

            try
            {
                var nonQuery = $"CREATE TABLE {tab.Value} " +
                        "(FIELD_1 varchar(30) PRIMARY KEY, " +
                        "FIELD_2 varchar(30), " +
                        "FIELD_3 varchar(30), " +
                        "FIELD_4 varchar(30), " +
                        "FIELD_5 varchar(30)) ";

                _bulkSqlLoader.ExecuteNonQuery(nonQuery);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Cannot create test table {tab.Value}");
                Debug.WriteLine(ex.Message);

                throw;
            }
        }

        /// <summary>
        /// Gets column names of env:table set in appsettings.json (table to use).
        /// </summary>
        protected void GetColumnsName()
        {
            var tab = _envirnoment.GetSection("table");

            Assert.True(tab != null, "appsetting.json must contains a table name in the section env:table");

            try
            {
                var results = _bulkSqlLoader.GetColumnsName(tab.Value);

                Assert.True(results.Any(), "inserted value does not match total value");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute countQuery in appsettings.json.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        protected long Count()
        {
            long result = -1;

            var query = _queries.GetSection("countQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:countQuery");

            try
            {
                result = Convert.ToInt64(_bulkSqlLoader
                   .ExecuteQuery(query.Value)
                   .FirstOrDefault()?
                   .FirstOrDefault());

                Debug.WriteLine($"Count: {result}");

                Assert.True((int)result >= 0);

                return result;
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);

                return result;
            }
        }

        /// <summary>
        /// Execute delAllQuery in appsettings.json.
        /// </summary>
        protected void DeleteAll()
        {
            var query = _queries.GetSection("delAllQuery");

            Assert.True(query != null, "appsetting.json must contains a delete query in the section queries:delAllQuery");

            try
            {
                _bulkSqlLoader.ExecuteNonQuery(query.Value);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }

            Assert.True(true);
        }

        /// <summary>
        /// Execute selectQuery in appsettings.json.
        /// </summary>
        protected void Select()
        {
            var query = _queries.GetSection("SelectQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:selectQuery");

            SingleInsert();
            try
            {
                var results = _bulkSqlLoader.ExecuteQuery(query.Value);

                Assert.True(results.Any());
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute selectQuery in appsettings.json and returns a table records instances list.
        /// </summary>
        protected void SelectParamterized()
        {
            var query = _queries.GetSection("SelectQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:selectQuery");

            SingleInsert();
            SingleInsert(new object[] { "2", DBNull.Value, "b", "c", "d" });

            try
            {
                var results = _bulkSqlLoader.ExecuteQuery<TEST_TABLE>(query.Value);

                Assert.True(results.Any());
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute insertQuery in appsettings.json.
        /// </summary>
        protected void SingleInsert(object[] parameters = null)
        {
            parameters ??= ["1", "a", "b", "c", "d"];

            var query = _queries.GetSection("insertQuery");

            Assert.True(query != null, "appsetting.json must contains a insert query in the section queries:insertQuery");

            try
            {
                int? result = _bulkSqlLoader.ExecuteNonQuery(query.Value,
                    parameters);

                Assert.True(result > 0);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Generates 200.000 non queries to test, it use env:table set in appsettings.json (table to use).
        /// It depends by DeleteAll and Count methods.
        /// </summary>
        protected void MassiveInsert()
        {
            Random rng = new();

            int queryNumber = rng.Next(200000);
            const int columnsTestTable = 5;

            var tab = _envirnoment.GetSection("table");

            Debug.WriteLine($"queryNumber: {queryNumber}");

            Assert.True(tab != null, "appsetting.json must contains a table name in the section env:table");

            int parametersIndex = -1;
            var sb = new StringBuilder();

            for (int i = 0; i < queryNumber; ++i)
            {
                string nonQuery =
                    $"INSERT INTO {tab.Value} " +
                    "(FIELD_1, FIELD_2, FIELD_3, FIELD_4, FIELD_5) VALUES (" +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex})";

                sb.AppendLine(nonQuery);
            }

            var nonQueries = sb.ToString().Split("\r\n");

            nonQueries = nonQueries
                .Where((_, i) => i != nonQueries.Length - 1)
                .ToArray();

            var parameters = new object[queryNumber * columnsTestTable]
                .Select((_, i) => (i as object))
                .ToArray();

            DeleteAll();
            try
            {
                _bulkSqlLoader.ExecuteNonQueries(nonQueries, parameters);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }

            long count = Count();

            Assert.True(count == queryNumber, "inserted value does not match total value");
        }

        #region Async versions

        /// <summary>
        /// Gets column names of env:table set in appsettings.json (table to use), async version.
        /// </summary>
        protected async Task GetColumnsNameAsync()
        {
            var tab = _envirnoment.GetSection("table");

            Assert.True(tab != null, "appsetting.json must contains a table name in the section env:table");

            try
            {
                var results = await _bulkSqlLoader.GetColumnsNameAsync(tab.Value)
                    .ConfigureAwait(false);

                Assert.True(results.Any(), "inserted value does not match total value");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute countQuery in appsettings.json, async version.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        protected async Task<long> CountAsync()
        {
            long result = -1;

            var query = _queries.GetSection("countQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:countQuery");

            try
            {
                result = Convert.ToInt64((await _bulkSqlLoader
                   .ExecuteQueryAsync(query.Value)
                   .ConfigureAwait(false))
                   ?.FirstOrDefault()
                   ?.FirstOrDefault());

                Debug.WriteLine($"Count: {result}");

                Assert.True((int)result >= 0);

                return result;
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);

                return result;
            }
        }

        /// <summary>
        /// Execute delAllQuery in appsettings.json, async version.
        /// </summary>
        protected async Task DeleteAllAsync()
        {
            var query = _queries.GetSection("delAllQuery");

            Assert.True(query != null, "appsetting.json must contains a delete query in the section queries:delAllQuery");

            try
            {
                await _bulkSqlLoader.ExecuteNonQueryAsync(query.Value)
                    .ConfigureAwait(false);

                Assert.True(true);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute selectQuery in appsettings.json and returns a table records instances list, async version.
        /// </summary>
        protected async Task SelectAsync()
        {
            var query = _queries.GetSection("SelectQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:selectQuery");

            await SingleInsertAsync()
                .ConfigureAwait(false);

            try
            {
                var results = await _bulkSqlLoader.ExecuteQueryAsync(query.Value)
                    .ConfigureAwait(false);

                Assert.True(results.Any());
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Execute insertQuery in appsettings.json, async version.
        /// </summary>
        protected async Task SingleInsertAsync()
        {
            var query = _queries.GetSection("insertQuery");

            Assert.True(query != null, "appsetting.json must contains a insert query in the section queries:insertQuery");

            try
            {
                int? result = await _bulkSqlLoader.ExecuteNonQueryAsync(query.Value,
                    new object[] { "1", "2", "3", "4", "5" })
                    .ConfigureAwait(false);

                Assert.True(result > 0);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        /// <summary>
        /// Generates 200.000 non queries to test, it use env:table set in appsettings.json (table to use), async version.
        /// It depends by Delete and Count methods.
        /// </summary>
        protected async Task MassiveInsertAsync()
        {
            Random rng = new();

            int queryNumber = rng.Next(200000);
            const int columnsTestTable = 5;

            var tab = _envirnoment.GetSection("table");

            Debug.WriteLine($"queryNumber: {queryNumber}");

            Assert.True(tab != null, "appsetting.json must contains a table name in the section env:table");

            int parametersIndex = -1;
            var sb = new StringBuilder();

            for (int i = 0; i < queryNumber; ++i)
            {
                string nonQuery =
                    $"INSERT INTO {tab.Value} " +
                    "(FIELD_1, FIELD_2, FIELD_3, FIELD_4, FIELD_5) VALUES (" +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex})";

                sb.AppendLine(nonQuery);
            }

            var nonQueries = sb.ToString().Split("\r\n");

            nonQueries = nonQueries
                .Where((_, i) => i != nonQueries.Length - 1)
                .ToArray();

            var parameters = new object[queryNumber * columnsTestTable]
                .Select((_, i) => (i as object))
                .ToArray();

            await DeleteAllAsync()
                .ConfigureAwait(false);

            try
            {
                await _bulkSqlLoader.ExecuteNonQueriesAsync(nonQueries, parameters)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }

            long count = (long)(await CountAsync()
                .ConfigureAwait(false));

            Assert.True(count == queryNumber, "inserted value does not match total value");
        }

        /// <summary>
        /// Execute selectQuery in appsettings.json and returns a table records instances list, async version.
        /// </summary>
        protected async Task SelectParamterizedAsync()
        {
            var query = _queries.GetSection("SelectQuery");

            Assert.True(query != null, "appsetting.json must contains a select count query in the section queries:selectQuery");

            SingleInsert();
            SingleInsert(new object[] { "2", "a", "b", "c", "d" });

            try
            {
                var results = await _bulkSqlLoader.ExecuteQueryAsync<TEST_TABLE>(query.Value)
                    .ConfigureAwait(false);

                Assert.True(results.Any());
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        #endregion Async versions
    }
}