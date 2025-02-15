/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector;

import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.extension.JdbcProvider;
import io.trino.spi.type.TypeManager;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class ConnectorContextInstance
        implements ConnectorContext
{
    private final NodeManager nodeManager;
    private final VersionEmbedder versionEmbedder;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final Supplier<ClassLoader> duplicatePluginClassLoaderFactory;
    /**
     * [feature] Supplying JDBC Providers to plugins
     * <p>
     * 20210811 shenlongguang github.com/ifengkou
     */
    private final JdbcProvider jdbcProvider;

    public ConnectorContextInstance(
            NodeManager nodeManager,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory,
            JdbcProvider jdbcProvider)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.duplicatePluginClassLoaderFactory = requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
        this.jdbcProvider = jdbcProvider;
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public VersionEmbedder getVersionEmbedder()
    {
        return versionEmbedder;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public ClassLoader duplicatePluginClassLoader()
    {
        return duplicatePluginClassLoaderFactory.get();
    }

    @Override
    public JdbcProvider getJdbcProvider()
    {
        return jdbcProvider;
    }
}
