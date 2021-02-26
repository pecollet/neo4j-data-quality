package neo4j.dq;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class PoolExtensionFactory extends ExtensionFactory<PoolExtensionFactory.Dependencies> {

    public PoolExtensionFactory() {
        super(ExtensionType.GLOBAL, "DQ_THREAD_POOL");
    }

    public interface Dependencies {
        GlobalProcedures globalProceduresRegistry();
        LogService log();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new ThreadPool(dependencies.log(), dependencies.globalProceduresRegistry());
    }

}

