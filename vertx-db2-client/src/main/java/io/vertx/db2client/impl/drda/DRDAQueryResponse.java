package io.vertx.db2client.impl.drda;

import io.netty.buffer.ByteBuf;

public class DRDAQueryResponse extends DRDAResponse {
    
    public DRDAQueryResponse(ByteBuf buffer, CCSIDManager ccsidManager) {
        super(buffer, ccsidManager);
    }

}
