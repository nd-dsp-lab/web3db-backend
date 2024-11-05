using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Query.API.Kernel.Util.Http.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Util.Http
{
    public class ClientApiProxy : IClientApiProxy
    {
        #region Attributes
        private readonly HttpClient _client;
        private readonly ILogger<ClientApiProxy> _logger;
        #endregion
        #region Contructor
        public ClientApiProxy(ILogger<ClientApiProxy> logger, HttpClient client)
        {
            _logger = logger;
            _client = client;
        }
        #endregion
        #region Methods
        #region Protected
        protected async Task<T> Response<T>(HttpResponseMessage response, string url)
        {
            try
            {
                if ((response.StatusCode != System.Net.HttpStatusCode.OK) && (response.StatusCode != System.Net.HttpStatusCode.NoContent))
                {
                    _logger.LogInformation($"Error {response.StatusCode}|{url}");
                    response.EnsureSuccessStatusCode();
                }

                var code = await response.Content.ReadAsStringAsync();
                var dto = JsonConvert.DeserializeObject<T>(code);

                return dto;
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, $"Message: {ex.Message}");
                throw;
            }
        }
        #endregion
        #region Public
        public virtual async Task<TOutput> GetAsync<TOutput>(string action, params object[] args)
        {
            try
            {
                var response = await _client.GetAsync(new Uri(_client.BaseAddress, string.Format(action, args)));
                await response.EnsureSuccessStatusCodeAsync();

                return await Response<TOutput>(response, _client.BaseAddress + action);


            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"URL: {_client.BaseAddress} Message: {ex.Message}");
                throw;
            }
        }
        public virtual async Task<TOutput> PostAsJsonAsync<TOutput, TInput>(TInput input, string action, params object[] args)
        {
            try
            {
                var json = JsonConvert.SerializeObject(input);

                var stringContent = new StringContent(json, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await _client.PostAsync(new Uri(_client.BaseAddress, string.Format(action, args)), stringContent).ConfigureAwait(false);
                await response.EnsureSuccessStatusCodeAsync();

                var content = await response.Content.ReadAsStringAsync();
                var obj = JsonConvert.DeserializeObject<TOutput>(content);

                return obj;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"URL: {new Uri(_client.BaseAddress + string.Format(action, args))}. Messages: {ex.Message}");
                throw;
            }
        }
        public virtual async Task<TOutput> DeleteAsync<TOutput>(string action, params object[] args)
        {
            try
            {
                var response = await _client.DeleteAsync(new Uri(_client.BaseAddress + string.Format(action, args)));
                await response.EnsureSuccessStatusCodeAsync();

                return await Response<TOutput>(response, _client.BaseAddress + action);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"URL: {_client.BaseAddress}. Message: {ex.Message}");
                throw;
            }
        }
        public virtual async Task<TOutput> PatchAsJsonAsync<TOutput, TInput>(TInput input, string action, params object[] args)
        {
            try
            {
                var json = JsonConvert.SerializeObject(input);

                var stringContent = new StringContent(json, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await _client.PatchAsync(new Uri(_client.BaseAddress, string.Format(action, args)), stringContent).ConfigureAwait(false);
                await response.EnsureSuccessStatusCodeAsync();

                var content = await response.Content.ReadAsStringAsync();
                var obj = JsonConvert.DeserializeObject<TOutput>(content);

                return obj;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"URL: {new Uri(_client.BaseAddress + string.Format(action, args))}. Message: {ex.Message}");
                throw;
            }
        }
        #endregion
        #endregion


    }
}
