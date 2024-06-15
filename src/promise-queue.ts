// deno-lint-ignore-file no-explicit-any

type Status = 'enqueued' | 'awaiting' | 'resolved' | 'rejected';

export class PromiseQueue<T = any> {
  private queue: (() => Promise<T>)[] = [];
  private queuePos = 0;
  private concurrentCounter = 0;
  private finalized: boolean = false;
  private resolvedOrRejected: boolean = false;
  private resolveAll: (results: T[]) => void;
  private rejectAll: (reason: any) => void;
  private concurrency: number;
  private promiseAll: Promise<T[]>;
  private results: T[] = [];

  constructor(concurrency: number) {
    const { promise, resolve, reject } = Promise.withResolvers<T[]>();
    this.promiseAll = promise;
    this.resolveAll = resolve;
    this.rejectAll = reject;
    this.promiseAll.finally(() => {
      this.resolvedOrRejected = true;
      this.queue.length = 0; // empty the queue
    });
    this.concurrency = concurrency;
  }

  add(operation: () => Promise<any>) {
    if (this.finalized) {
      throw new Error(
        'Attempted to add to PromiseQueue that has already been finalized.',
      );
    }
    this.queue.push(operation);
  }

  /**
   * Run the next concurrent operation in the queue.
   */
  private next() {
    if (this.queuePos >= this.queue.length) {
      // nothing left to execute
      return;
    }

    ++this.concurrentCounter;
    const fn = this.queue[this.queuePos];
    const fnQueuePos = this.queuePos;
    ++this.queuePos;
    const promise = fn(); // start the operation
    promise.then((result: T) => {
      --this.concurrentCounter;
      this.results[fnQueuePos] = result;
      if (this.queuePos < this.queue.length) {
        setTimeout(() => {
          this.next();
        }, 0);
      } else if (!this.resolvedOrRejected && this.concurrentCounter === 0) {
        this.resolveAll(this.results);
      }
    }).catch((reason: any) => {
      // done, because of error
      --this.concurrentCounter;
      if (!this.resolvedOrRejected) {
        this.rejectAll(reason);
      }
    });
  }

  execute(): Promise<T[]> {
    if (this.finalized) {
      // already started, await on the global promise
      return this.promiseAll;
    }
    this.finalized = true;

    for (let i = 0; i < this.concurrency; ++i) {
      this.next();
    }

    return this.promiseAll;
  }
}
