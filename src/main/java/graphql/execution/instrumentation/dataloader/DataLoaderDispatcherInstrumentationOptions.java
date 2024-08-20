package graphql.execution.instrumentation.dataloader;

import graphql.PublicApi;

/**
 * WARNING: This class does not exist anymore in graphql-java 22
 * We include this shell of a class so that kickstart doesn't complain, but
 * it should not actually be used.
 */
@PublicApi
public class DataLoaderDispatcherInstrumentationOptions {

    private final boolean includeStatistics;

    private DataLoaderDispatcherInstrumentationOptions(boolean includeStatistics) {
        this.includeStatistics = includeStatistics;
    }

    public static DataLoaderDispatcherInstrumentationOptions newOptions() {
        return new DataLoaderDispatcherInstrumentationOptions(false);
    }

    /**
     * This will toggle the ability to include java-dataloader statistics into the extensions
     * output of your query
     *
     * @param flag the switch to follow
     *
     * @return a new options object
     */
    public DataLoaderDispatcherInstrumentationOptions includeStatistics(boolean flag) {
        return new DataLoaderDispatcherInstrumentationOptions(flag);
    }


    public boolean isIncludeStatistics() {
        return includeStatistics;
    }

}