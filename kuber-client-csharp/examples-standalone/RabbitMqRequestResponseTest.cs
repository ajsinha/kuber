/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber RabbitMQ Request/Response Test Client (v1.7.5)
 * 
 * Tests the Kuber Request/Response messaging feature via RabbitMQ AMQP.
 * 
 * Prerequisites:
 * - RabbitMQ running on localhost:5672
 * - Kuber server running with messaging enabled
 * - RabbitMQ.Client NuGet package:
 *     dotnet add package RabbitMQ.Client
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Kuber.Client.Examples
{
    /// <summary>
    /// Test client for Kuber's RabbitMQ-based Request/Response messaging.
    /// </summary>
    public class RabbitMqRequestResponseTest
    {
        // Configuration
        private const string RabbitMqHost = "localhost";
        private const int RabbitMqPort = 5672;
        private const string RabbitMqUser = "guest";
        private const string RabbitMqPass = "guest";
        private const string RabbitMqVhost = "/";
        
        private const string RequestQueue = "ccs_cache_request";
        private const string ResponseQueue = "ccs_cache_response";
        
        // Your Kuber API key
        private const string ApiKey = "kub_your_api_key_here";
        private const string TestRegion = "default";
        
        // RabbitMQ objects
        private IConnection? _connection;
        private IModel? _producerChannel;
        private IModel? _consumerChannel;
        
        // State
        private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode>> _pendingRequests = new();
        private readonly List<JsonNode> _receivedResponses = new();
        private readonly object _responseLock = new();
        
        public static async Task Main(string[] args)
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║  Kuber RabbitMQ Request/Response Test Client (C#)                  ║");
            Console.WriteLine("║  Tests cache operations via RabbitMQ AMQP                          ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            
            var client = new RabbitMqRequestResponseTest();
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
            Console.WriteLine($"RabbitMQ Host: {RabbitMqHost}:{RabbitMqPort}");
            Console.WriteLine($"Virtual Host: {RabbitMqVhost}");
            Console.WriteLine($"Request Queue: {RequestQueue}");
            Console.WriteLine($"Response Queue: {ResponseQueue}");
            Console.WriteLine();
            
            try
            {
                Connect();
                StartResponseConsumer();
                
                await Task.Delay(1000);
                
                await RunTestSuiteAsync();
                
                Console.WriteLine("\nWaiting for responses (15 seconds)...");
                await Task.Delay(15000);
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
            Console.WriteLine("Connecting to RabbitMQ...");
            
            var factory = new ConnectionFactory
            {
                HostName = RabbitMqHost,
                Port = RabbitMqPort,
                UserName = RabbitMqUser,
                Password = RabbitMqPass,
                VirtualHost = RabbitMqVhost
            };
            
            _connection = factory.CreateConnection();
            _producerChannel = _connection.CreateModel();
            _consumerChannel = _connection.CreateModel();
            
            // Declare queues
            _producerChannel.QueueDeclare(RequestQueue, durable: true, exclusive: false, autoDelete: false);
            _consumerChannel.QueueDeclare(ResponseQueue, durable: true, exclusive: false, autoDelete: false);
            
            Console.WriteLine("✓ Connected to RabbitMQ");
        }
        
        private void StartResponseConsumer()
        {
            _consumerChannel!.BasicQos(0, 10, false);
            
            var consumer = new EventingBasicConsumer(_consumerChannel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var response = JsonNode.Parse(body);
                    if (response == null) return;
                    
                    // Parse nested response
                    var requestNode = response["request"];
                    var responseNode = response["response"];
                    
                    var messageId = requestNode?["message_id"]?.GetValue<string>() ?? "unknown";
                    var success = responseNode?["success"]?.GetValue<bool>() ?? false;
                    var operation = requestNode?["operation"]?.GetValue<string>() ?? "unknown";
                    var processingTime = response["processing_time_ms"]?.GetValue<long>() ?? 0;
                    
                    Console.WriteLine();
                    Console.WriteLine("════════════════════════════════════════════════════════════");
                    Console.WriteLine("📥 RESPONSE RECEIVED");
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
                    
                    _consumerChannel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error processing message: {ex.Message}");
                    _consumerChannel.BasicNack(ea.DeliveryTag, false, false);
                }
            };
            
            _consumerChannel.BasicConsume(ResponseQueue, false, consumer);
            Console.WriteLine("✓ Response consumer started");
        }
        
        private async Task RunTestSuiteAsync()
        {
            Console.WriteLine("\n════════════════════════════════════════════════════════════");
            Console.WriteLine("STARTING TEST SUITE");
            Console.WriteLine("════════════════════════════════════════════════════════════");
            
            // Test 1: SET
            Console.WriteLine("\n--- Test 1: SET operation ---");
            var setRequest = CreateRequest("SET", TestRegion, "rabbitmq-test:user:1001");
            setRequest["value"] = new JsonObject
            {
                ["name"] = "John Doe",
                ["email"] = "john@example.com",
                ["broker"] = "rabbitmq"
            };
            setRequest["ttl"] = 300;
            PublishRequest(setRequest);
            await Task.Delay(1000);
            
            // Test 2: GET
            Console.WriteLine("\n--- Test 2: GET operation ---");
            PublishRequest(CreateRequest("GET", TestRegion, "rabbitmq-test:user:1001"));
            await Task.Delay(1000);
            
            // Test 3: EXISTS
            Console.WriteLine("\n--- Test 3: EXISTS operation ---");
            PublishRequest(CreateRequest("EXISTS", TestRegion, "rabbitmq-test:user:1001"));
            await Task.Delay(1000);
            
            // Test 4: SET another
            Console.WriteLine("\n--- Test 4: SET another key ---");
            var setRequest2 = CreateRequest("SET", TestRegion, "rabbitmq-test:user:1002");
            setRequest2["value"] = new JsonObject
            {
                ["name"] = "Jane Smith",
                ["role"] = "admin"
            };
            PublishRequest(setRequest2);
            await Task.Delay(1000);
            
            // Test 5: KEYS
            Console.WriteLine("\n--- Test 5: KEYS operation ---");
            PublishRequest(CreateRequest("KEYS", TestRegion, "rabbitmq-test:*"));
            await Task.Delay(1000);
            
            // Test 6: DELETE
            Console.WriteLine("\n--- Test 6: DELETE operation ---");
            PublishRequest(CreateRequest("DELETE", TestRegion, "rabbitmq-test:user:1002"));
            await Task.Delay(1000);
            
            // Test 7: GET non-existent
            Console.WriteLine("\n--- Test 7: GET non-existent key ---");
            PublishRequest(CreateRequest("GET", TestRegion, "rabbitmq-test:nonexistent:key"));
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
            
            var props = _producerChannel!.CreateBasicProperties();
            props.ContentType = "application/json";
            props.DeliveryMode = 2;
            props.MessageId = messageId;
            
            var body = Encoding.UTF8.GetBytes(requestJson);
            _producerChannel.BasicPublish("", RequestQueue, props, body);
            
            Console.WriteLine("   ✓ Sent");
        }
        
        private void Shutdown()
        {
            Console.WriteLine("\nShutting down...");
            
            try
            {
                _producerChannel?.Close();
                _consumerChannel?.Close();
                _connection?.Close();
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
