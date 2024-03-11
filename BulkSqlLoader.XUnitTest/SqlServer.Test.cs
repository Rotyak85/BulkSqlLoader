using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace Loaders.Test
{
    public class SqlServerTest : BulkSqlLoaderTest
    {
        public SqlServerTest()
        {
            _engineName = "SqlServer";

            var connectionString = _connectionStrings.GetSection("SqlServer:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new SqlConnection(connectionString);

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
        internal void SqlServerGetColumnsName()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.GetColumnsName();
        }

        [SkippableFact]
        internal object SqlServerCount()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            return base.Count();
        }

        [SkippableFact]
        internal void SqlServerDeleteAll()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.DeleteAll();
        }

        [SkippableFact]
        internal void SqlServerSingleInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.SingleInsert();
        }

        [SkippableFact]
        internal void SqlServerMassiveInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.MassiveInsert();
        }

        [SkippableFact]
        internal void SqlServerSelect()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.Select();
        }

        [SkippableFact]
        internal void SqlServerSelectParametrized()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.SelectParamterized();
        }

        #region Async versions

        [SkippableFact]
        internal async Task SqlServerCountAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.CountAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task SqlServerDeleteAllAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.DeleteAllAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task SqlServerSingleInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SingleInsertAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task SqlServerMassiveInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.MassiveInsertAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task SqlServerSelectAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SelectAsync()
                .ConfigureAwait(false);
        }

        #endregion Async versions
    }
}