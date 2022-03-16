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
package io.trino.dynamiccatalog;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * https://github.com/prestodb/presto/pull/12605
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/5
 */
public class ResponseParser
{
    @JsonProperty("message")
    public String message;
    @JsonProperty("statusCode")
    public int statusCode;
    @JsonProperty("data")
    public Object data;

    public ResponseParser build(String message, int statusCode)
    {
        this.message = message;
        this.statusCode = statusCode;
        this.data = null;
        return this;
    }

    public ResponseParser build(String message, int statusCode, Object obj)
    {
        this.message = message;
        this.statusCode = statusCode;
        this.data = obj;
        return this;
    }

    public ResponseParser()
    {
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public void setStatusCode(int statusCode)
    {
        this.statusCode = statusCode;
    }

    public Object getData()
    {
        return data;
    }

    public void setData(Object data)
    {
        this.data = data;
    }
}
