using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace BulkSqlLoader.XUnitTest
{
    public class SqlServerTest : BulkSqlLoader
    {
        public SqlServerTest()
        {
            _engineName = "SqlServer";

            var connectionString = _connectionStrings.GetSection("SqlServer:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new SqlConnection(connectionString);

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
        internal object SqlServerCount()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            return base.Count();
        }

        [SkippableFact]
        internal void SqlServerDelete()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.Delete();
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
        internal async Task SqlServerCountAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.CountAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task SqlServerDeleteAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.DeleteAsync()
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
    }
}