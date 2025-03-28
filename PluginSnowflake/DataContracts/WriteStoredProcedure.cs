using PluginSnowflake.API.Utility;

namespace PluginSnowflake.DataContracts
{
    public class WriteStoredProcedure
    {
        public string SchemaName { get; set; }
        public string RoutineName { get; set; }

        public string GetId()
        {
            return $"{Utility.GetSafeName(SchemaName)}.{Utility.GetSafeName(RoutineName)}";
        }
    }
}