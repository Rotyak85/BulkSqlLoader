using Npgsql;
using System;
using System.Data;
using System.Threading.Tasks;
using Xunit;

namespace Loaders.Test
{
    public class PostgreSqlTest : BulkSqlLoaderTest
    {
        public PostgreSqlTest()
        {
            _engineName = "Postgres";

            var connectionString = _connectionStrings.GetSection("Postgres:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new NpgsqlConnection(connectionString);

            _bulkSqlLoader = new BulkSqlLoader(
                conn,
                throwException: true, paramsBatchLimit:
                paramsBatchLimit);

            try
            {
                base.InitializeEnvironment();
            }
            catch (Exception ex)
            {
                Assert.Fail($"Cannot intitialize test for {_engineName}: {ex.Message}");
            }
        }

        [SkippableFact]
        internal void PostgresGetColumnsName()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.GetColumnsName();
        }

        [SkippableFact]
        internal object PostgresCount()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            return base.Count();
        }

        [SkippableFact]
        internal void PostgresDeleteAll()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.DeleteAll();
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
        internal void PostgresSelect()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.Select();
        }

        [SkippableFact]
        internal void PostgresSelectParametrized()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.SelectParamterized();
        }

        #region Async versions

        [SkippableFact]
        internal async Task PostgresCountAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.CountAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task PostgresDeleteAllAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.DeleteAllAsync()
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

        [SkippableFact]
        internal async Task PostgresSelectAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SelectAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task PostgresSelectParametrizedAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SelectParamterizedAsync()
                .ConfigureAwait(false);
        }

        #endregion Async versions
    }
}