package neo4j.dq;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.*;
import java.util.stream.Stream;

public class ThreadPool extends LifecycleAdapter  {

    private ExecutorService executorService;
    private final Log log;
    private final GlobalProcedures globalProceduresRegistry;

    public ThreadPool (LogService log, GlobalProcedures globalProceduresRegistry) {
        this.log = log.getInternalLog(ThreadPool.class);
        this.globalProceduresRegistry = globalProceduresRegistry;

        // expose this config instance via `@Context ThreadPool pool` injection
        globalProceduresRegistry.registerComponent((Class<ThreadPool>) getClass(), ctx -> this, true);
        this.log.info("successfully registered Pools for @Context");
    }

    @Override
    public void init() {
        ThreadFactory threadFactory = r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        };
        int threads = Runtime.getRuntime().availableProcessors() * 2;
        int queueSize = threads * 5;
        this.executorService = new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                threadFactory);
    }

    @Override
    public void shutdown() throws Exception {
        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {

        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
