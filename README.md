# BulkSqlLoader
A library for massive insertion of queries with multiple parameters

BulkSqlLoader allows you to easily handle large command queries with multiple parameters and bypass the limitations of various servers

Here are the limit parameter numbers for some servers:

	Ingres : 1024
	Microsoft Access : 768
	Oracle : 32767
	PostgreSQL : 32767
	SQLite : 999
	SQL Server : 2100 (depending on the version)
	Sybase ASE : 2000

	Taken from:
    - https://www.jooq.org/doc/3.12/manual/sql-building/dsl-context/custom-settings/settings-inline-threshold/


Initialization:


            var connectionString = _connectionStrings.GetSection("Mysql:connectionString").Value;
            var paramsBatchLimit = Convert.ToInt32(_envirnoment.GetSection("paramsBatchLimit").Value);

            IDbConnection conn = new SqlConnection(connectionString);

            _bulkSqlLoader = new BulkSqlLoader(
                conn,
                throwException: true, paramsBatchLimit:
                paramsBatchLimit); //for example: sqlserver 2017 express has 2100

Use:
    
            try
            {
                await _bulkSqlLoader.ExecuteNonQueriesAsync(nonQueries, parameters)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
               //excpetion handler code
            }
                  
The following example is just to show how the "nonQueries", "parameters" args can be structured
            
            /*example: generate 200000 non queries with 1M random parameters*/
            
            Random rng = new();
            int queryNumber = rng.Next(200000);
            
            const int columnsTestTable = 5;

            int parametersIndex = -1;
            
            var sb = new StringBuilder();
            for (int i = 0; i < queryNumber; ++i)
            {
                string nonQuery =
                    $"INSERT INTO YOUR_TABLE " +
                    "(FIELD_1, FIELD_2, FIELD_3, FIELD_4, FIELD_5) VALUES (" +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}, " +
                    $"@p{++parametersIndex}) ";

                sb.AppendLine(nonQuery);
            }

            var nonQueries = sb.ToString().Split("\r\n");

            nonQueries = nonQueries
                .Where((_, i) => i != nonQueries.Length - 1)
                .ToArray();

            var parameters = new object[queryNumber * columnsTestTable]
                .Select((_, i) => (i as object))
                .ToArray();
