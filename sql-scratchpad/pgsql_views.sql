-- BigQuery
SELECT pg_get_viewdef('k3l_collect_nft', true);

SELECT concat(profile.profile_id, '-', publication_collect_module_collected_records.collect_publication_nft_id) AS collect_nft_id,
  profile.profile_id,
  publication_collect_module_collected_records.collect_publication_nft_id AS pub_id,
  "substring"(publication_collect_module_collected_records.publication_id::text, 1, POSITION(('-'::text) IN (publication_collect_module_collected_records.publication_id)) - 1) AS to_profile_id,
  "substring"(publication_collect_module_collected_records.publication_id::text, POSITION(('-'::text) IN (publication_collect_module_collected_records.publication_id)) + 1) AS to_pub_id,
  publication_collect_module_collected_records.record_id AS metadata,
  publication_collect_module_collected_records.block_timestamp AS created_at
  FROM publication_collect_module_collected_records
    LEFT JOIN profile ON publication_collect_module_collected_records.collected_by::text = profile.owned_by::text;

-- 

SELECT pg_get_viewdef('k3l_comments', true);

SELECT post_comment.comment_id,
  post_comment.comment_by_profile_id AS profile_id,
  post_comment.contract_publication_id AS pub_id,
  "substring"(post_comment.post_id::text, 1, POSITION(('-'::text) IN (post_comment.post_id)) - 1) AS to_profile_id,
  "substring"(post_comment.post_id::text, POSITION(('-'::text) IN (post_comment.post_id)) + 1) AS to_pub_id,
  post_comment.content_uri,
  post_comment.block_timestamp AS created_at
  FROM post_comment;

-- 

SELECT pg_get_viewdef('k3l_follows', true);

SELECT profile.profile_id,
  follower.follow_profile_id AS to_profile_id,
  follower.block_timestamp AS created_at
  FROM follower
    LEFT JOIN profile ON follower.address::text = profile.owned_by::text;

-- 

SELECT pg_get_viewdef('k3l_mirrors', true);

SELECT related_posts.post_id,
  related_posts.profile_id,
  related_posts.pub_id,
  "substring"(related_posts.is_related_to::text, 1, POSITION(('-'::text) IN (related_posts.is_related_to)) - 1) AS to_profile_id,
  "substring"(related_posts.is_related_to::text, POSITION(('-'::text) IN (related_posts.is_related_to)) + 1) AS to_pub_id,
  related_posts.block_timestamp AS created_at
  FROM ( SELECT profile_post.post_id,
          profile_post.profile_id,
          profile_post.contract_publication_id AS pub_id,
          COALESCE(profile_post.is_related_to_post, profile_post.is_related_to_comment) AS is_related_to,
          profile_post.block_timestamp
          FROM profile_post
        WHERE profile_post.is_related_to_post IS NOT NULL OR profile_post.is_related_to_comment IS NOT NULL) related_posts;

-- 

SELECT pg_get_viewdef('k3l_posts', true);

SELECT profile_post.post_id,
  profile_post.profile_id,
  profile_post.contract_publication_id AS pub_id,
  profile_post.content_uri,
  profile_post.block_timestamp AS created_at
  FROM profile_post;

-- 

SELECT pg_get_viewdef('k3l_profiles', true);

SELECT profile.profile_id,
  profile.owned_by AS owner_address,
  profile.handle,
  profile.profile_picture_s3_url AS image_uri,
  profile.metadata_block_timestamp AS created_at
  FROM profile;

-- 

SELECT pg_get_viewdef('k3l_follow_counts', true);

SELECT k3l_follows.profile_id,
  count(*) AS count
  FROM k3l_follows
GROUP BY k3l_follows.profile_id;