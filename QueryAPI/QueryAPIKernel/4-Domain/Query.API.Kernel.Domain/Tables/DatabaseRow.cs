using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Domain.Tables
{
    public class DatabaseRow
    {
        public IDictionary<string, object> Fields { get; set; }
    }
}
