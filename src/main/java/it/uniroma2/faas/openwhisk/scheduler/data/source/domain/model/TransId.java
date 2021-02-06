package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.deserializer.TransIdDeserializer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.serializer.TransIdSerializer;

import java.util.List;

@JsonDeserialize(using = TransIdDeserializer.class)
@JsonSerialize(using = TransIdSerializer.class)
public class TransId {

    /*
   {
    "transid":[
      "rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu",
      1609810319398
     ]
     }
     */

    /*
    {
    "transid":[
      "bxytKe9Qk3EYVEYQnpHs02NDFo5eKAd3",
      1609870677855,
      [
         "2rkHZR02ErZB6kol4tl5LN4oxiHB5VH8",
         1609870676410,
         [
            "iRFErOK5Hflz8qduy49vBKWWMFTG44IW",
            1609870676273
         ]
      ]
   ]
   }
     */

    // list of transaction indexed from most recent to old once
    private final List<Transaction> transactions;

    public TransId(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    @Override
    public String toString() {
        return "TransId{" +
                "transactions=" + transactions +
                '}';
    }

}