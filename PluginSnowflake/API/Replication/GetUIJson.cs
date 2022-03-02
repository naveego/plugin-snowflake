using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {"ui:order", new []
                {
                    "SchemaName",
                    "GoldenTableName",
                    "VersionTableName",
                    "ShouldCreateSchema"
                }}
            };

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}