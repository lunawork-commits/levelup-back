-- Fix NULL timestamps in branch_clientvkstatus
-- Backfill *_joined_at and story_uploaded_at with checked_at when the boolean flag is TRUE
-- Run per tenant schema

DO $$
DECLARE
    schema_name TEXT;
BEGIN
    FOR schema_name IN
        SELECT schema_name FROM public.clients_company
        WHERE schema_name NOT IN ('public', 'dev')
    LOOP
        -- story_uploaded_at: backfill with checked_at when is_story_uploaded = true
        EXECUTE format(
            'UPDATE %I.branch_clientvkstatus
             SET story_uploaded_at = checked_at
             WHERE is_story_uploaded = true AND story_uploaded_at IS NULL',
            schema_name
        );

        -- community_joined_at: backfill with checked_at when is_community_member = true
        EXECUTE format(
            'UPDATE %I.branch_clientvkstatus
             SET community_joined_at = checked_at
             WHERE is_community_member = true AND community_joined_at IS NULL',
            schema_name
        );

        -- newsletter_joined_at: backfill with checked_at when is_newsletter_subscriber = true
        EXECUTE format(
            'UPDATE %I.branch_clientvkstatus
             SET newsletter_joined_at = checked_at
             WHERE is_newsletter_subscriber = true AND newsletter_joined_at IS NULL',
            schema_name
        );

        RAISE NOTICE 'Fixed schema: %', schema_name;
    END LOOP;
END $$;
