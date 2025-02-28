using System.Threading.Tasks;
using Aunalytics.Sdk.Plugins;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Write
{
    public static partial class Write
    {
        private static readonly string GetStoredProcedureParamsQuery = @"select ARGUMENT_SIGNATURE from INFORMATION_SCHEMA.PROCEDURES where PROCEDURE_NAME='{0}'";

        public static async Task<Schema> GetSchemaForStoredProcedureAsync(IConnectionFactory connFactory,
            WriteStoredProcedure storedProcedure)
        {
            var schema = new Schema
            {
                Id = storedProcedure.GetId(),
                Name = storedProcedure.GetId(),
                Description = "",
                DataFlowDirection = Schema.Types.DataFlowDirection.Write,
                Query = storedProcedure.GetId()
            };

            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(
                string.Format(GetStoredProcedureParamsQuery, storedProcedure.RoutineName),
                conn);
            var reader = await cmd.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                var paramSignature = reader.GetValueById("ARGUMENT_SIGNATURE").ToString();
                if(paramSignature.StartsWith("(") && paramSignature.EndsWith(")")){ 
                    paramSignature = paramSignature.Substring(1, paramSignature.Length - 2);
                }
                var paramSignatureSplit = paramSignature.Split(',');
                foreach (var param in paramSignatureSplit)
                {
                    try
                    {
                        // split by whitespace
                        var paramSplit = param.Trim().Split(' ');
                        var property = new Property
                        {
                            Id = paramSplit[0],
                            Name = paramSplit[0],
                            Description = "",
                            Type = Discover.Discover.GetType(paramSplit[1]),
                            TypeAtSource = paramSplit[1]
                        };

                        schema.Properties.Add(property);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }

            await conn.CloseAsync();

            return schema;
        }
    }
}