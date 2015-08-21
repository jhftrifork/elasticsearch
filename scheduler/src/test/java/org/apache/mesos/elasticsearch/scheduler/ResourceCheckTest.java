package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for ResourceCheck
 */
public class ResourceCheckTest {

    private static final String CPU_NAME = Resources.cpus(10).getName();

    private Protos.Offer mockOfferWithResources(Protos.Resource... resources) {
        Protos.Offer.Builder mockBuilder = Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("y645wt43q").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("ue9310ei2910").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("y54qr32e23t54").build())
                .setHostname("foobar");
        for (Protos.Resource resource : resources) {
            mockBuilder.addResources(resource);
        }
        return mockBuilder.build();
    }

    @Test
    public void givenExactResourceShouldAccept() {
        assertTrue(new ResourceOfferCheck(CPU_NAME, 1, "Not enough CPU").checkOffer(mockOfferWithResources(Resources.cpus(1))).isEmpty());
    }

    @Test
    public void givenMoreResourceShouldAccept() {
        assertTrue(new ResourceOfferCheck(CPU_NAME, 1, "Not enough CPU").checkOffer(mockOfferWithResources(Resources.cpus(10))).isEmpty());
    }

    @Test
    public void givenMultipleResourceShouldAccept() {
        assertTrue(new ResourceOfferCheck(CPU_NAME, 1, "Not enough CPU").checkOffer(mockOfferWithResources(Resources.mem(10), Resources.disk(10), Resources.cpus(10))).isEmpty());
    }

    @Test
    public void givenTooLittleResourceShouldDeny() {
        assertFalse(new ResourceOfferCheck(CPU_NAME, 1, "Not enough CPU").checkOffer(mockOfferWithResources(Resources.cpus(0.1))).isEmpty());
    }
}