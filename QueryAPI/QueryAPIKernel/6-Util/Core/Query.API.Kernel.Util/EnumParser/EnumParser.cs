using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Util.EnumParser
{
    public static class EnumParser
    {
        #region Methods
        public static string GetName<T>(T value) where T : System.Enum
        {
            if (value == null) return null;
            return Enum.GetName(typeof(T), value);
        }
        public static T Parse<T>(string value) where T : struct, System.Enum
            => System.Enum.Parse<T>(value);

        public static T? ParseNullable<T>(string value) where T : struct, System.Enum
        {
            if (string.IsNullOrEmpty(value)) return null;
            return System.Enum.Parse<T>(value);
        }
        #endregion

    }
}
