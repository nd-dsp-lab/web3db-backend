using Query.API.Kernel.Domain.Core.Entities.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Domain.Core.Entities
{
    public abstract class AbstractEntity<TEntity, TId> : IEntity<TEntity> where TEntity : class
    {
        public virtual TId Id { get; set; }
        protected AbstractEntity()
        {
        }
    }
}
