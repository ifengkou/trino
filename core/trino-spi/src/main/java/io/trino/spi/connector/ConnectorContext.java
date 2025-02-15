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
package io.trino.spi.connector;

import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.extension.JdbcProvider;
import io.trino.spi.type.TypeManager;

public interface ConnectorContext
{
    default NodeManager getNodeManager()
    {
        throw new UnsupportedOperationException();
    }

    default VersionEmbedder getVersionEmbedder()
    {
        throw new UnsupportedOperationException();
    }

    default TypeManager getTypeManager()
    {
        throw new UnsupportedOperationException();
    }

    default PageSorter getPageSorter()
    {
        throw new UnsupportedOperationException();
    }

    default PageIndexerFactory getPageIndexerFactory()
    {
        throw new UnsupportedOperationException();
    }

    default ClassLoader duplicatePluginClassLoader()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * [feature] Supplying JDBC Providers to plugins
     * Avoid instantiating multiple JDBC connection pools
     * <p>
     * 20210811 shenlongguang github.com/ifengkou
     */
    default JdbcProvider getJdbcProvider()
    {
        throw new UnsupportedOperationException();
    }
}
