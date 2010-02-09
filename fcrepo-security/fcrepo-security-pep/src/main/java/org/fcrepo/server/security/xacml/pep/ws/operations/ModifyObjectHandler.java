/*
 * File: ModifyObjectHandler.java
 *
 * Copyright 2007 Macquarie E-Learning Centre Of Excellence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.server.security.xacml.pep.ws.operations;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.axis.AxisFault;
import org.apache.axis.MessageContext;
import org.apache.log4j.Logger;

import org.fcrepo.common.Constants;
import org.fcrepo.server.security.xacml.pep.PEPException;
import org.fcrepo.server.security.xacml.util.LogUtil;

import com.sun.xacml.attr.AnyURIAttribute;
import com.sun.xacml.attr.AttributeValue;
import com.sun.xacml.attr.StringAttribute;
import com.sun.xacml.ctx.RequestCtx;


/**
 * @author nishen@melcoe.mq.edu.au
 */
public class ModifyObjectHandler
        extends AbstractOperationHandler {

    private static Logger log =
            Logger.getLogger(ModifyObjectHandler.class.getName());

    public ModifyObjectHandler()
            throws PEPException {
        super();
    }

    public RequestCtx handleResponse(MessageContext context)
            throws OperationHandlerException {
        return null;
    }

    public RequestCtx handleRequest(MessageContext context)
            throws OperationHandlerException {
        log.debug("ModifyObjectHandler/handleRequest!");

        RequestCtx req = null;
        List<Object> oMap = null;

        String pid = null;
        String state = null;
        // String label = null;
        String ownerId = null;
        // String logMessage = null;

        try {
            oMap = getSOAPRequestObjects(context);
            log.debug("Retrieved SOAP Request Objects");
        } catch (AxisFault af) {
            log.error("Error obtaining SOAP Request Objects", af);
            throw new OperationHandlerException("Error obtaining SOAP Request Objects",
                                                af);
        }

        try {
            pid = (String) oMap.get(0);
            state = (String) oMap.get(1);
            // label = (String) oMap.get(2);
            ownerId = (String) oMap.get(3);
            // logMessage = (String) oMap.get(4);
        } catch (Exception e) {
            log.error("Error obtaining parameters", e);
            throw new OperationHandlerException("Error obtaining parameters.",
                                                e);
        }

        log.debug("Extracted SOAP Request Objects");

        Map<URI, AttributeValue> actions = new HashMap<URI, AttributeValue>();
        Map<URI, AttributeValue> resAttr = new HashMap<URI, AttributeValue>();

        try {
            if (pid != null && !"".equals(pid)) {
                resAttr.put(Constants.OBJECT.PID.getURI(),
                            new StringAttribute(pid));
            }
            if (pid != null && !"".equals(pid)) {
                resAttr.put(new URI(XACML_RESOURCE_ID),
                            new AnyURIAttribute(new URI(pid)));
            }
            if (state != null && !"".equals(state)) {
                resAttr.put(Constants.OBJECT.STATE.getURI(),
                            new StringAttribute(state));
            }
            if (ownerId != null && !"".equals(ownerId)) {
                resAttr.put(Constants.OBJECT.OWNER.getURI(),
                            new StringAttribute(state));
            }

            if (state != null && state.equals("A")) {
                actions.put(Constants.ACTION.ID.getURI(),
                            new StringAttribute("publish"));
            } else if (state != null && state.equals("I")) {
                actions.put(Constants.ACTION.ID.getURI(),
                            new StringAttribute("unpublish"));
            } else {
                actions.put(Constants.ACTION.ID.getURI(),
                            new StringAttribute(Constants.ACTION.MODIFY_OBJECT
                                    .getURI().toASCIIString()));
            }
            actions.put(Constants.ACTION.API.getURI(),
                        new StringAttribute(Constants.ACTION.APIM.getURI()
                                .toASCIIString()));

            req =
                    getContextHandler().buildRequest(getSubjects(context),
                                                     actions,
                                                     resAttr,
                                                     getEnvironment(context));

            LogUtil.statLog(context.getUsername(),
                            Constants.ACTION.MODIFY_OBJECT.getURI()
                                    .toASCIIString(),
                            pid,
                            null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new OperationHandlerException(e.getMessage(), e);
        }

        return req;
    }
}