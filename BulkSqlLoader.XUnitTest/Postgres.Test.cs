using System;
using System.Data;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace BulkSqlLoader.XUnitTest
{
    public class PostgreSqlTest : BulkSqlLoader
    {
        public PostgreSqlTest()
        {
            _engineName = "Postgres";

            var connectionString = _connectionStrings.GetSection("Postgres:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new NpgsqlConnection(connectionString);

            _bulkSqlLoader = new Core.BulkSqlLoader(
                conn,
                throwException: true, paramsBatchLimit:
                paramsBatchLimit);

            try
            {
                base.InitializeEnvironment();
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Cannot intitialize test for {_engineName}: {ex.Message}");
            }
        }

        [SkippableFact]
        internal object PostgresCount()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            return base.CountAsync();
        }

        [SkippableFact]
        internal void PostgresDelete()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.Delete();
        }

        [SkippableFact]
        internal void PostgresSingleInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.SingleInsert();
        }

        [SkippableFact]
        internal void PostgresMassiveInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.MassiveInsert();
        }

        [SkippableFact]
        internal async Task PostgresCountAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.CountAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task PostgresDeleteAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.DeleteAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task PostgresSingleInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SingleInsertAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task PostgresMassiveInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.MassiveInsertAsync()
                .ConfigureAwait(false);
        }
    }
}