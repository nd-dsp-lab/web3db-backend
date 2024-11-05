using Query.API.Kernel.Domain.Core.Entities.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.DataAccess.Persistence.Repositories.Interfaces
{
    public interface IRepository<T, TId> where T : class, IEntity<T>
    {
        Task<T> FindIdAsync(TId id);
        Task<IEnumerable<T>> GetAll();
        Task<T> SaveAsync(T entity);
        Task<T> Add(T entity);
        Task<IList<T>> SaveAsync(IList<T> entities);
        Task<T> UpdateAsync(T entity);
        Task<IList<T>> UpdateListAsync(IList<T> entities);
        Task<bool> DeleteAsync(T entity);
    }
}
