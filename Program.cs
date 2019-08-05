using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmark.TxManager
{
    class Program
    {

        static int concurrency = Environment.ProcessorCount;
        static int seed = Environment.TickCount;

        static async Task Main(string[] args)
        {
            Console.WriteLine("Current Warmup");
            var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            IAmqpTransactionManager txManager = new AmqpTransactionManager();
            var tasks = new List<Task>(concurrency);
            for (int i = 0; i < concurrency; i++)
            {
                tasks.Add(Warmup(tokenSource.Token, txManager));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);

            Console.WriteLine("Current warmup done");
            Console.ReadLine();

            tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            tasks.Clear();
            for (int i = 0; i < concurrency; i++)
            {
                tasks.Add(KeepBusy(tokenSource.Token, txManager));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Current done");

            Console.ReadLine();

            Console.WriteLine("LockFree Warmup");
            tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            txManager = new LockFreeAmqpTransactionManager();
            tasks.Clear();
            for (int i = 0; i < concurrency; i++)
            {
                tasks.Add(Warmup(tokenSource.Token, txManager));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Lockfree warmup done");
            Console.ReadLine();

            tasks.Clear();
            tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            for (int i = 0; i < concurrency; i++)
            {
                tasks.Add(KeepBusy(tokenSource.Token, txManager));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("LockFree done");
        }

        static async Task Warmup(CancellationToken token, IAmqpTransactionManager manager)
        {
            while (!token.IsCancellationRequested)
            {
                var tx = new Transaction();
                await manager.EnlistAsync(tx, new ServiceBusConnection()).ConfigureAwait(false);
                manager.RemoveEnlistment(tx.TransactionInformation.LocalIdentifier);
            }
        }

        static async Task KeepBusy(CancellationToken token, IAmqpTransactionManager manager)
        {
            var random = new Random(Interlocked.Increment(ref seed));

            await Task.Delay(random.Next(10, 100)).ConfigureAwait(false);

            while (!token.IsCancellationRequested)
            {
                var tx = new Transaction();
                var serviceBusConnection = new ServiceBusConnection();
                await Task.WhenAll(manager.EnlistAsync(tx, serviceBusConnection), manager.EnlistAsync(tx, serviceBusConnection), manager.EnlistAsync(tx, serviceBusConnection));
                manager.RemoveEnlistment(tx.TransactionInformation.LocalIdentifier);
            }
        }
    }

    class AmqpTransactionEnlistment
    {
        private Transaction transaction;
        private IAmqpTransactionManager amqpTransactionManager;
        private ServiceBusConnection serviceBusConnection;

        public AmqpTransactionEnlistment(Transaction transaction, IAmqpTransactionManager amqpTransactionManager, ServiceBusConnection serviceBusConnection)
        {
            this.transaction = transaction;
            this.amqpTransactionManager = amqpTransactionManager;
            this.serviceBusConnection = serviceBusConnection;
        }

        public ArraySegment<byte> AmqpTransactionId { get; internal set; }

        internal Task<AmqpTransactionEnlistment> GetOrCreateAsync(object operationTimeout)
        {
            return Task.FromResult(new AmqpTransactionEnlistment(this.transaction, this.amqpTransactionManager, this.serviceBusConnection));
        }
    }

    class Transaction
    {
        public IsolationLevel IsolationLevel { get; internal set; } = IsolationLevel.Serializable;
        public TransactionInformation TransactionInformation { get; internal set; } = new TransactionInformation();

        internal bool EnlistPromotableSinglePhase(AmqpTransactionEnlistment transactionEnlistment)
        {
            return true;
        }
    }

    class TransactionInformation
    {
        public string LocalIdentifier { get; internal set; } = Guid.NewGuid().ToString();
    }

    class ServiceBusConnection
    {
        public TimeSpan OperationTimeout { get; internal set; }
    }

    internal interface IAmqpTransactionManager
    {
        Task<ArraySegment<byte>> EnlistAsync(Transaction transaction, ServiceBusConnection serviceBusConnection);
        void RemoveEnlistment(string transactionId);
    }

    internal class AmqpTransactionManager : IAmqpTransactionManager
    {
        readonly object syncRoot = new object();
        readonly Dictionary<string, AmqpTransactionEnlistment> enlistmentMap = new Dictionary<string, AmqpTransactionEnlistment>(StringComparer.Ordinal);

        public async Task<ArraySegment<byte>> EnlistAsync(
            Transaction transaction,
            ServiceBusConnection serviceBusConnection)
        {
            if (transaction.IsolationLevel != IsolationLevel.Serializable)
            {
                throw new InvalidOperationException($"The only supported IsolationLevel is {nameof(IsolationLevel.Serializable)}");
            }

            string transactionId = transaction.TransactionInformation.LocalIdentifier;
            AmqpTransactionEnlistment transactionEnlistment;

            lock (this.syncRoot)
            {
                if (!this.enlistmentMap.TryGetValue(transactionId, out transactionEnlistment))
                {
                    transactionEnlistment = new AmqpTransactionEnlistment(transaction, this, serviceBusConnection);
                    this.enlistmentMap.Add(transactionId, transactionEnlistment);

                    if (!transaction.EnlistPromotableSinglePhase(transactionEnlistment))
                    {
                        this.enlistmentMap.Remove(transactionId);
                        throw new InvalidOperationException("Local transactions are not supported with other resource managers/DTC.");
                    }
                }
            }

            transactionEnlistment = await transactionEnlistment.GetOrCreateAsync(serviceBusConnection.OperationTimeout).ConfigureAwait(false);
            return transactionEnlistment.AmqpTransactionId;
        }

        public void RemoveEnlistment(string transactionId)
        {
            lock (this.syncRoot)
            {
                this.enlistmentMap.Remove(transactionId);
            }
        }
    }

    internal class LockFreeAmqpTransactionManager : IAmqpTransactionManager
    {
        readonly ConcurrentDictionary<string, AmqpTransactionEnlistment> enlistmentMap = new ConcurrentDictionary<string, AmqpTransactionEnlistment>(StringComparer.Ordinal);

        public async Task<ArraySegment<byte>> EnlistAsync(
            Transaction transaction,
            ServiceBusConnection serviceBusConnection)
        {
            if (transaction.IsolationLevel != IsolationLevel.Serializable)
            {
                throw new InvalidOperationException($"The only supported IsolationLevel is {nameof(IsolationLevel.Serializable)}");
            }

            string transactionId = transaction.TransactionInformation.LocalIdentifier;
            AmqpTransactionEnlistment transactionEnlistment;

            if (!this.enlistmentMap.TryGetValue(transactionId, out transactionEnlistment))
            {
                transactionEnlistment = new AmqpTransactionEnlistment(transaction, this, serviceBusConnection);
                this.enlistmentMap.TryAdd(transactionId, transactionEnlistment);

                if (!transaction.EnlistPromotableSinglePhase(transactionEnlistment))
                {
                    this.enlistmentMap.TryRemove(transactionId, out var _);
                    throw new InvalidOperationException("Local transactions are not supported with other resource managers/DTC.");
                }
            }

            transactionEnlistment = await transactionEnlistment.GetOrCreateAsync(serviceBusConnection.OperationTimeout).ConfigureAwait(false);
            return transactionEnlistment.AmqpTransactionId;
        }

        public void RemoveEnlistment(string transactionId)
        {
            this.enlistmentMap.TryRemove(transactionId, out var _);
        }
    }

    internal class LockFreeCrazyAmqpTransactionManager : IAmqpTransactionManager
    {
        readonly ConcurrentDictionary<string, AmqpTransactionEnlistment> enlistmentMap = new ConcurrentDictionary<string, AmqpTransactionEnlistment>(StringComparer.Ordinal);

        public async Task<ArraySegment<byte>> EnlistAsync(
            Transaction transaction,
            ServiceBusConnection serviceBusConnection)
        {
            if (transaction.IsolationLevel != IsolationLevel.Serializable)
            {
                throw new InvalidOperationException($"The only supported IsolationLevel is {nameof(IsolationLevel.Serializable)}");
            }

            string transactionId = transaction.TransactionInformation.LocalIdentifier;
            bool fresh = false;
            AmqpTransactionEnlistment transactionEnlistment = this.enlistmentMap.GetOrAdd(transactionId, txId =>
            {
                fresh = true;
                return new AmqpTransactionEnlistment(transaction, this, serviceBusConnection);
            });

            if (fresh && !transaction.EnlistPromotableSinglePhase(transactionEnlistment))
            {
                this.enlistmentMap.TryRemove(transactionId, out var _);
                throw new InvalidOperationException("Local transactions are not supported with other resource managers/DTC.");
            }

            transactionEnlistment = await transactionEnlistment.GetOrCreateAsync(serviceBusConnection.OperationTimeout).ConfigureAwait(false);
            return transactionEnlistment.AmqpTransactionId;
        }

        public void RemoveEnlistment(string transactionId)
        {
            this.enlistmentMap.TryRemove(transactionId, out var _);
        }
    }
}
