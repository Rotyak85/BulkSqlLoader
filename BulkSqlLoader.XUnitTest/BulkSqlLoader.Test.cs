using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace BulkSqlLoader.XUnitTest
{
    public abstract partial class BulkSqlLoader
    {
        protected string _engineName;

        protected readonly IConfigurationSection _connectionStrings;
        protected readonly IConfigurationSection _queries;
        protected readonly IConfigurationSection _envirnoment;

        protected Core.BulkSqlLoader _bulkSqlLoader;

        protected BulkSqlLoader()
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
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);

                return result;
            }

            Debug.WriteLine($"Count: {result}");

            Assert.True((int)result >= 0);

            return result;
        }

        protected void Delete()
        {
            var query = _queries.GetSection("delAllQuery");

            Assert.True(query != null, "appsetting.json must contains a delete query in the section queries:delAllQuery");

            try
            {
                _bulkSqlLoader.ExecuteNonQuery(query.Value);
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            Assert.True(true);
        }

        protected void SingleInsert()
        {
            int? result = -1;

            var query = _queries.GetSection("insertQuery");

            Assert.True(query != null, "appsetting.json must contains a insert query in the section queries:insertQuery");

            try
            {
                result = _bulkSqlLoader.ExecuteNonQuery(query.Value,
                    new object[] { "1", "2", "3", "4", "5" });
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            Assert.True(result >= 0);
        }

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

            Delete();
            try
            {
                _bulkSqlLoader.ExecuteNonQueries(nonQueries, parameters);
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            long count = Count();

            Assert.True(count == queryNumber, "inserted value does not match total value");
        }
    }

    public abstract partial class BulkSqlLoader
    {
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
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);

                return result;
            }

            Debug.WriteLine($"Count: {result}");

            Assert.True((int)result >= 0);

            return result;
        }

        protected async Task DeleteAsync()
        {
            var query = _queries.GetSection("delAllQuery");

            Assert.True(query != null, "appsetting.json must contains a delete query in the section queries:delAllQuery");

            try
            {
                await _bulkSqlLoader.ExecuteNonQueryAsync(query.Value)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            Assert.True(true);
        }

        protected async Task SingleInsertAsync()
        {
            int? result = -1;

            var query = _queries.GetSection("insertQuery");

            Assert.True(query != null, "appsetting.json must contains a insert query in the section queries:insertQuery");

            try
            {
                result = await _bulkSqlLoader.ExecuteNonQueryAsync(query.Value,
                    new object[] { "1", "2", "3", "4", "5" })
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            Assert.True(result >= 0);
        }

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

            await DeleteAsync()
                .ConfigureAwait(false);

            try
            {
                await _bulkSqlLoader.ExecuteNonQueriesAsync(nonQueries, parameters)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Assert.True(false, ex.Message);
            }

            long count = (long)(await CountAsync()
                .ConfigureAwait(false));

            Assert.True(count == queryNumber, "inserted value does not match total value");
        }
    }
}