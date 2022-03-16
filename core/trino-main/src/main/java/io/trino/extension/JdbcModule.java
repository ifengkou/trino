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
package io.trino.extension;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import io.trino.spi.extension.JdbcProvider;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/8/3
 */
public class JdbcModule
        implements Module
{
    private static final Logger log = Logger.get(JdbcModule.class);

    @Override
    public void configure(Binder binder)
    {
        log.info("--- init base jdbc provider ---");
        binder.bind(JdbcProvider.class).to(BaseJdbcProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcConfig.class);
    }
}
