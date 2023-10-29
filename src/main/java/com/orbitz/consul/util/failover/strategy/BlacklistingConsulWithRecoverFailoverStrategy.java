package com.orbitz.consul.util.failover.strategy;

import com.google.common.net.HostAndPort;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Provide recover chance for instance in black list.
 * @author liaoyushao
 */
public class BlacklistingConsulWithRecoverFailoverStrategy implements ConsulFailoverStrategy {

    // The map of blacklisted addresses
    private Map<HostAndPort, Instant> blacklist = Collections.synchronizedMap(new HashMap<>());

    // The map of viable targets
    private Collection<HostAndPort> targets;

    // The blacklist timeout
    private long timeout;

    /**
     * Constructs a blacklisting strategy with a collection of hosts and ports
     * @param targets
     *            A set of viable hosts
     */
    public BlacklistingConsulWithRecoverFailoverStrategy(Collection<HostAndPort> targets, long timeout) {
        this.targets = targets;
        this.timeout = timeout;
    }

    /**
     * Check is request viable.
     * Only return false when targets are empty.
     * {@inheritDoc}
     */
    @Override
    public boolean isRequestViable(@NotNull Request current) {
        return targets.size() != 0;
    }

    @Override
    public void markRequestFailed(@NotNull Request current) {
        this.blacklist.put(fromRequest(current), Instant.now());
    }

    @Override
    public Optional<Request> computeNextStage(Request previousRequest, Response previousResponse) {

        final HostAndPort initialTarget = fromRequest(previousRequest);

        if (isPreviousResponseFail(previousResponse))
            this.blacklist.put(initialTarget, Instant.now());

        if (blacklist.containsKey(initialTarget)) {

            Optional<HostAndPort> optionalNextHostAndPort = targets.stream().filter(target -> {

                if (!blacklist.containsKey(target)) {
                    return true;
                }

                if (isTargetAbleToBeRemovedFromBlackList(target)) {
                    blacklist.remove(target);
                    return true;
                }
                else {
                    return false;
                }

            }).findAny();

            if (!optionalNextHostAndPort.isPresent()) {
                return Optional.empty();
            }

            HostAndPort next = optionalNextHostAndPort.get();

            // Construct the next URL using the old parameters (ensures we don't have to do a copy-on-write)
            final HttpUrl nextURL = previousRequest.url().newBuilder().host(next.getHost()).port(next.getPort()).build();

            // Return the result
            return Optional.of(previousRequest.newBuilder().url(nextURL).build());
        }
        else {

            // Construct the next URL using the old parameters (ensures we don't have to do
            // a copy-on-write
            final HttpUrl nextURL = previousRequest.url().newBuilder().host(initialTarget.getHost()).port(initialTarget.getPort()).build();

            // Return the result
            return Optional.of(previousRequest.newBuilder().url(nextURL).build());
        }

    }

    /**
     * Check if prev response fail.
     * A 404 does NOT indicate a failure in this case, so it should never blacklist the previous target.
     */
    private boolean isPreviousResponseFail(Response previousResponse) {
        return (previousResponse != null) && !previousResponse.isSuccessful() && !(previousResponse.code() == 404);
    }

    private boolean isTargetAbleToBeRemovedFromBlackList(HostAndPort target) {

        if (!blacklist.containsKey(target)) {
            return false;
        }

        final Instant blacklistWhen = blacklist.get(target);

        return !Duration.between(blacklistWhen, Instant.now()).minusMillis(timeout).isNegative();
    }

    /**
     * Reconstructs a HostAndPort instance from the request object
     */
    private HostAndPort fromRequest(Request request) {
        return HostAndPort.fromParts(request.url().host(), request.url().port());
    }

}
