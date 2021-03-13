package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.TransId;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Transaction;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

public class TransIdSerializer extends StdSerializer<TransId> {

    public TransIdSerializer() {
        this(null);
    }

    public TransIdSerializer(Class<TransId> t) {
        super(t);
    }

    @Override
    public void serialize(@Nonnull TransId transId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        List<Transaction> transactions = transId.getTransactions();
        writeTransactions(transactions, 0, jsonGenerator);
    }

    private void writeTransactions(@Nonnull List<Transaction> transactions, int index, JsonGenerator jsonGenerator)
            throws IOException {
        if (transactions.isEmpty()) {
            jsonGenerator.writeStartArray();
            jsonGenerator.writeEndArray();
            return;
        } else if (index > transactions.size() - 1) {
            return;
        }

        jsonGenerator.writeStartArray();
        jsonGenerator.writeString(transactions.get(index).getId());
        jsonGenerator.writeNumber(transactions.get(index).getTimestamp());
        writeTransactions(transactions, ++index, jsonGenerator);
        jsonGenerator.writeEndArray();
    }

}