using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Util.Http.Interfaces
{
    public interface IClientApiProxy
    {
        Task<TOutput> GetAsync<TOutput>(string action, params object[] args);
        Task<TOutput> PostAsJsonAsync<TOutput, TInput>(TInput input, string action, params object[] args);
        Task<TOutput> PatchAsJsonAsync<TOutput, TInput>(TInput input, string action, params object[] args);
        Task<TOutput> DeleteAsync<TOutput>(string action, params object[] args);
    }
}
