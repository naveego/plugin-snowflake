using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginMySQL.API.Write
{
    public static partial class Write
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {"ui:order", new []
                {
                    "StoredProcedure"
                }}
            };
            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}