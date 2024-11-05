using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Domain.Core.Entities.Interfaces
{
    public interface IEntity<in T> where T : class
    {
    }
}
