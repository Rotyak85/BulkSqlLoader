using System;
using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Xunit;

namespace BulkSqlLoader.XUnitTest
{
    public class MysqlTest : BulkSqlLoader
    {
        public MysqlTest()
        {
            _engineName = "Mysql";

            var connectionString = _connectionStrings.GetSection("Mysql:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new MySqlConnection(connectionString);

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
        internal object MysqlCount()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            return base.CountAsync();
        }

        [SkippableFact]
        internal void MysqlDelete()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.Delete();
        }

        [SkippableFact]
        internal void MysqlSingleInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.SingleInsert();
        }

        [SkippableFact]
        internal void MysqlMassiveInsert()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            base.MassiveInsert();
        }

        [SkippableFact]
        internal async Task MysqlCountAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.CountAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task MysqlDeleteAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.DeleteAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task MysqlSingleInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.SingleInsertAsync()
                .ConfigureAwait(false);
        }

        [SkippableFact]
        internal async Task MysqlMassiveInsertAsync()
        {
            var isDifferent = !string.Equals(_envirnoment.GetSection("Engine").Value, _engineName, StringComparison.CurrentCultureIgnoreCase);
            Skip.If(isDifferent);

            await base.MassiveInsertAsync()
                .ConfigureAwait(false);
        }
    }
}