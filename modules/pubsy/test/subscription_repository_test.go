package test

import (
	"eventura/modules/pubsy/core"
	"eventura/modules/pubsy/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func TestInsertAndGetSubscriptionInfo(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	logger, _ := zap.NewDevelopment()
	repo := internal.NewSubscriptionRepositoryImpl(db, logger)

	subscriptionInfo := &core.SubscriptionInfo{
		Topic:              "test-topic",
		Consumer:           "test-consumer",
		LastFetchedOffset:  42,
		LastConsumedOffset: 11,
	}

	// Act
	err := repo.InsertSubscriptionInfo(subscriptionInfo)
	require.NoError(t, err, "InsertSubscriptionInfo should not return an error")

	// Retrieve by ID
	retrievedInfo, err := repo.GetSubscriptionInfo(subscriptionInfo.ID)
	require.NoError(t, err, "GetSubscriptionInfo should not return an error")
	require.NotNil(t, retrievedInfo, "Retrieved subscription info should not be nil")

	// Retrieve by topic and consumer
	t.Logf("Looking for topic: %s, consumer: %s", subscriptionInfo.Topic, subscriptionInfo.Consumer)
	dumpDb(db)
	foundInfo, err := repo.FindSubscriptionInfoByTopicAndConsumer(subscriptionInfo.Topic, subscriptionInfo.Consumer)
	t.Logf("Found info: %+v, error: %v", foundInfo, err)

	// Assert for GetSubscriptionInfo
	assert.Equal(t, subscriptionInfo.ID, retrievedInfo.ID, "IDs should match")
	assert.Equal(t, subscriptionInfo.Topic, retrievedInfo.Topic, "Topics should match")
	assert.Equal(t, subscriptionInfo.Consumer, retrievedInfo.Consumer, "Consumers should match")
	assert.Equal(t, subscriptionInfo.LastFetchedOffset, retrievedInfo.LastFetchedOffset, "LastFetchedOffset should match")
	assert.Equal(t, subscriptionInfo.LastConsumedOffset, retrievedInfo.LastConsumedOffset, "LastConsumedOffset should match")

	// Assert for FindSubscriptionInfoByTopicAndConsumer
	assert.Equal(t, subscriptionInfo.ID, foundInfo.ID, "IDs should match")
	assert.Equal(t, subscriptionInfo.Topic, foundInfo.Topic, "Topics should match")
	assert.Equal(t, subscriptionInfo.Consumer, foundInfo.Consumer, "Consumers should match")
	assert.Equal(t, subscriptionInfo.LastFetchedOffset, foundInfo.LastFetchedOffset, "LastFetchedOffset should match")
	assert.Equal(t, subscriptionInfo.LastConsumedOffset, foundInfo.LastConsumedOffset, "LastConsumedOffset should match")
}
