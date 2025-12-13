/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber IBM MQ Request/Response Test Client (v1.7.5)
 * 
 * Tests the Kuber Request/Response messaging feature via IBM MQ.
 * 
 * Prerequisites:
 * - IBM MQ running with queue manager accessible
 * - Kuber server running with messaging enabled
 * - IBMMQDotnetClient NuGet package:
 *     dotnet add package IBMMQDotnetClient
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using IBM.WMQ;

namespace Kuber.Client.Examples
{
    /// <summary>
    /// Test client for Kuber's IBM MQ-based Request/Response messaging.
    /// </summary>
    public class IbmMqRequestResponseTest
    {
        // Configuration
        private const string QueueManagerName = "QM1";
        private const string ChannelName = "DEV.APP.SVRCONN";
        private const string HostName = "localhost";
        private const int Port = 1414;
        
        private const string RequestQueueName = "ccs_cache_request";
        private const string ResponseQueueName = "ccs_cache_response";
        
        // Your Kuber API key
        private const string ApiKey = "kub_your_api_key_here";
        private const string TestRegion = "default";
        
        // MQ objects
        private MQQueueManager? _queueManager;
        private MQQueue? _requestQueue;
        private MQQueue? _responseQueue;
        
        // State
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode>> _pendingRequests = new();
        private readonly List<JsonNode> _receivedResponses = new();
        private readonly object _responseLock = new();
        
        public static async Task Main(string[] args)
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║  Kuber IBM MQ Request/Response Test Client (C#)                    ║");
            Console.WriteLine("║  Tests cache operations via IBM MQ messaging                       ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            
            var client = new IbmMqRequestResponseTest();
            try
            {
                await client.RunAsync();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
                Console.Error.WriteLine(ex.StackTrace);
            }
        }
        
        public async Task RunAsync()
        {
            Console.WriteLine($"Queue Manager: {QueueManagerName}");
            Console.WriteLine($"Channel: {ChannelName}");
            Console.WriteLine($"Host: {HostName}:{Port}");
            Console.WriteLine($"Request Queue: {RequestQueueName}");
            Console.WriteLine($"Response Queue: {ResponseQueueName}");
            Console.WriteLine();
            
            try
            {
                Connect();
                var consumerTask = StartResponseConsumerAsync(_cts.Token);
                
                await Task.Delay(1000);
                
                await RunTestSuiteAsync();
                
                Console.WriteLine("\nWaiting for responses (15 seconds)...");
                await Task.Delay(15000);
                
                _cts.Cancel();
                await consumerTask;
            }
            finally
            {
                Shutdown();
                PrintSummary();
            }
            
            Console.WriteLine("\nTest complete.");
        }
        
        private void Connect()
        {
            Console.WriteLine("Connecting to IBM MQ...");
            
            var properties = new Hashtable
            {
                { MQConstants.HOST_NAME_PROPERTY, HostName },
                { MQConstants.PORT_PROPERTY, Port },
                { MQConstants.CHANNEL_PROPERTY, ChannelName },
                { MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_MANAGED }
            };
            
            _queueManager = new MQQueueManager(QueueManagerName, properties);
            
            _requestQueue = _queueManager.AccessQueue(
                RequestQueueName,
                MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING
            );
            
            _responseQueue = _queueManager.AccessQueue(
                ResponseQueueName,
                MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING
            );
            
            Console.WriteLine("✓ Connected to IBM MQ");
        }
        
        private async Task StartResponseConsumerAsync(CancellationToken ct)
        {
            Console.WriteLine("✓ Response consumer started");
            
            await Task.Run(() =>
            {
                var gmo = new MQGetMessageOptions
                {
                    Options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING,
                    WaitInterval = 1000
                };
                
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var message = new MQMessage();
                        _responseQueue!.Get(message, gmo);
                        
                        var body = message.ReadString(message.MessageLength);
                        ProcessMessage(body);
                    }
                    catch (MQException ex) when (ex.ReasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE)
                    {
                        // Normal timeout, continue
                    }
                    catch (Exception ex)
                    {
                        if (!ct.IsCancellationRequested)
                        {
                            Console.Error.WriteLine($"Error receiving message: {ex.Message}");
                        }
                    }
                }
            }, ct);
        }
        
        private void ProcessMessage(string body)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine("════════════════════════════════════════════════════════════");
                Console.WriteLine("📥 RESPONSE RECEIVED");
                
                var response = JsonNode.Parse(body);
                if (response == null) return;
                
                var requestNode = response["request"];
                var responseNode = response["response"];
                
                var messageId = requestNode?["message_id"]?.GetValue<string>() ?? "unknown";
                var success = responseNode?["success"]?.GetValue<bool>() ?? false;
                var operation = requestNode?["operation"]?.GetValue<string>() ?? "unknown";
                var processingTime = response["processing_time_ms"]?.GetValue<long>() ?? 0;
                
                Console.WriteLine($"   Message ID: {messageId}");
                Console.WriteLine($"   Success: {success}");
                Console.WriteLine($"   Operation: {operation}");
                Console.WriteLine($"   Processing Time: {processingTime}ms");
                
                if (success)
                {
                    var result = responseNode?["result"]?.ToJsonString();
                    if (!string.IsNullOrEmpty(result))
                    {
                        Console.WriteLine(result.Length > 100 
                            ? $"   Result: {result[..100]}..." 
                            : $"   Result: {result}");
                    }
                }
                else
                {
                    var errorCode = responseNode?["error_code"]?.GetValue<string>() ?? "UNKNOWN";
                    var error = responseNode?["error"]?.GetValue<string>() ?? "Unknown error";
                    Console.WriteLine($"   Error Code: {errorCode}");
                    Console.WriteLine($"   Error: {error}");
                }
                Console.WriteLine("════════════════════════════════════════════════════════════");
                
                lock (_responseLock)
                {
                    _receivedResponses.Add(response);
                }
                
                if (_pendingRequests.TryRemove(messageId, out var tcs))
                {
                    tcs.SetResult(response);
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error processing message: {ex.Message}");
            }
        }
        
        private async Task RunTestSuiteAsync()
        {
            Console.WriteLine("\n════════════════════════════════════════════════════════════");
            Console.WriteLine("STARTING TEST SUITE");
            Console.WriteLine("════════════════════════════════════════════════════════════");
            
            // Test 1: SET
            Console.WriteLine("\n--- Test 1: SET operation ---");
            var setRequest = CreateRequest("SET", TestRegion, "ibmmq-test:user:1001");
            setRequest["value"] = new JsonObject
            {
                ["name"] = "John Doe",
                ["email"] = "john@example.com",
                ["broker"] = "ibmmq"
            };
            setRequest["ttl"] = 300;
            PublishRequest(setRequest);
            await Task.Delay(1000);
            
            // Test 2: GET
            Console.WriteLine("\n--- Test 2: GET operation ---");
            PublishRequest(CreateRequest("GET", TestRegion, "ibmmq-test:user:1001"));
            await Task.Delay(1000);
            
            // Test 3: EXISTS
            Console.WriteLine("\n--- Test 3: EXISTS operation ---");
            PublishRequest(CreateRequest("EXISTS", TestRegion, "ibmmq-test:user:1001"));
            await Task.Delay(1000);
            
            // Test 4: SET another
            Console.WriteLine("\n--- Test 4: SET another key ---");
            var setRequest2 = CreateRequest("SET", TestRegion, "ibmmq-test:user:1002");
            setRequest2["value"] = new JsonObject
            {
                ["name"] = "Jane Smith",
                ["role"] = "admin"
            };
            PublishRequest(setRequest2);
            await Task.Delay(1000);
            
            // Test 5: KEYS
            Console.WriteLine("\n--- Test 5: KEYS operation ---");
            PublishRequest(CreateRequest("KEYS", TestRegion, "ibmmq-test:*"));
            await Task.Delay(1000);
            
            // Test 6: DELETE
            Console.WriteLine("\n--- Test 6: DELETE operation ---");
            PublishRequest(CreateRequest("DELETE", TestRegion, "ibmmq-test:user:1002"));
            await Task.Delay(1000);
            
            // Test 7: GET non-existent
            Console.WriteLine("\n--- Test 7: GET non-existent key ---");
            PublishRequest(CreateRequest("GET", TestRegion, "ibmmq-test:nonexistent:key"));
            await Task.Delay(1000);
            
            Console.WriteLine("\n✓ All requests sent");
        }
        
        private JsonObject CreateRequest(string operation, string region, string key)
        {
            var messageId = $"req-{Guid.NewGuid().ToString()[..12]}";
            
            var request = new JsonObject
            {
                ["operation"] = operation,
                ["region"] = region,
                ["key"] = key,
                ["api_key"] = ApiKey,
                ["message_id"] = messageId
            };
            
            _pendingRequests[messageId] = new TaskCompletionSource<JsonNode>();
            return request;
        }
        
        private void PublishRequest(JsonObject request)
        {
            var messageId = request["message_id"]!.GetValue<string>();
            var requestJson = request.ToJsonString();
            
            Console.WriteLine("📤 SENDING REQUEST");
            Console.WriteLine($"   Message ID: {messageId}");
            Console.WriteLine($"   Operation: {request["operation"]}");
            Console.WriteLine($"   Key: {request["key"]}");
            
            var message = new MQMessage();
            message.WriteString(requestJson);
            message.Format = MQConstants.MQFMT_STRING;
            message.Persistence = MQConstants.MQPER_PERSISTENT;
            
            var pmo = new MQPutMessageOptions();
            _requestQueue!.Put(message, pmo);
            
            Console.WriteLine("   ✓ Sent");
        }
        
        private void Shutdown()
        {
            Console.WriteLine("\nShutting down...");
            
            try
            {
                _requestQueue?.Close();
                _responseQueue?.Close();
                _queueManager?.Disconnect();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error closing connections: {ex.Message}");
            }
        }
        
        private void PrintSummary()
        {
            Console.WriteLine("\n════════════════════════════════════════════════════════════");
            Console.WriteLine("TEST SUMMARY");
            Console.WriteLine("════════════════════════════════════════════════════════════");
            
            lock (_responseLock)
            {
                Console.WriteLine($"Total responses received: {_receivedResponses.Count}");
            }
            Console.WriteLine($"Pending requests (no response): {_pendingRequests.Count}");
            
            if (!_pendingRequests.IsEmpty)
            {
                Console.WriteLine("\nRequests without responses:");
                foreach (var messageId in _pendingRequests.Keys)
                {
                    Console.WriteLine($"  - {messageId}");
                }
            }
            
            int successCount, errorCount;
            lock (_responseLock)
            {
                successCount = _receivedResponses.Count(r =>
                {
                    var responseNode = r["response"];
                    return responseNode?["success"]?.GetValue<bool>() ?? false;
                });
                errorCount = _receivedResponses.Count - successCount;
            }
            
            Console.WriteLine($"\nSuccessful operations: {successCount}");
            Console.WriteLine($"Failed operations: {errorCount}");
            Console.WriteLine("════════════════════════════════════════════════════════════");
        }
    }
}
