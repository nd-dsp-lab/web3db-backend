using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Util.Exceptions
{
    public class PropertyNotFoundException : Exception
    {
        public string PropertyName { get; set; }

        public PropertyNotFoundException(string propertyName, string message) : base(message)
        {
            PropertyName = propertyName;
        }
    }
}
