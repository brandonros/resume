-- Locks a named resource, such as 'customer:42', until the calling transaction ends.
-- Postgres releases it at commit or rollback, including when the connection drops.
-- Names are hashed to 64 bits, so two names sharing a lock is possible but very unlikely.
create or replace function resume.lock_resource(p_resource text)
returns void
language sql
as $$
    select pg_advisory_xact_lock(hashtextextended(p_resource, 0));
$$;
