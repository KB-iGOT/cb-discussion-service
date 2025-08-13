package com.igot.cb.discussion.repository;

import com.igot.cb.discussion.entity.DiscussionAnswerPostReplyEntity;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface DiscussionAnswerPostReplyRepository extends JpaRepository<DiscussionAnswerPostReplyEntity, String> {
    @Modifying
    @Transactional
    @Query(value = "UPDATE discussion_answer_post_reply SET profanityresponse = cast(?2 as jsonb), isprofane = ?3 WHERE discussion_id = ?1", nativeQuery = true)
    void updateProfanityFieldsByDiscussionId(String discussionId, String profanityResponseJson, Boolean isProfane);

    @Query(
            value = "SELECT discussion_id, data, is_active, created_on, updated_on " +
                    "FROM discussion_answer_post_reply WHERE discussion_id = ?1",
            nativeQuery = true
    )
    Optional<DiscussionAnswerPostReplyEntity> findDiscussionAnswerPostReplyBasedOnDiscussionId(String discussionId);
}
