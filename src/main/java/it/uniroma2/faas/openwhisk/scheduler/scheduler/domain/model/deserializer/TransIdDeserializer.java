package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.TransId;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Transaction;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransIdDeserializer extends StdDeserializer<TransId> {

    public TransIdDeserializer() {
        this(null);
    }

    public TransIdDeserializer(Class<TransId> vc) {
        super(vc);
    }

    @Override
    public TransId deserialize(@Nonnull JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        List<Transaction> transactions = new ArrayList<>();
        while (node != null && node.size() >= 2) {
            String id = node.get(0).asText();
            Long timestamp = node.get(1).asLong();
            transactions.add(new Transaction(id, timestamp));
            node = node.get(2);
        }

        return new TransId(transactions);
    }

}