import libmongoc

/// An extension of `MongoCollection` encapsulating CRUD operations.
extension MongoCollection {
	/**
	 * Finds the documents in this collection which match the provided filter.
	 *
	 * - Parameters:
	 *   - filter: A `Document` that should match the query
	 *   - options: Optional `FindOptions` to use when executing the command
	 *
	 * - Returns: A `MongoCursor` over the resulting `Document`s
	 */
	public func find(_ filter: Document = [:], options: FindOptions? = nil) throws -> MongoCursor<CollectionType> {
		let opts = try BsonEncoder().encode(options)
		guard let cursor = mongoc_collection_find_with_opts(self._collection, filter.data, opts?.data, nil) else {
			throw MongoError.invalidResponse()
		}
		guard let client = self._client else {
			throw MongoError.invalidClient()
		}
		return MongoCursor(fromCursor: cursor, withClient: client)
	}

	/**
	 * Runs an aggregation framework pipeline against this collection.
	 *
	 * - Parameters:
	 *   - pipeline: an `[Document]` containing the pipeline of aggregation operations to perform
	 *   - options: Optional `AggregateOptions` to use when executing the command
	 *
	 * - Returns: A `MongoCursor` over the resulting `Document`s
	 */
	public func aggregate(_ pipeline: [Document], options: AggregateOptions? = nil) throws -> MongoCursor<Document> {
		let opts = try BsonEncoder().encode(options)
		let pipeline: Document = ["pipeline": pipeline]
		guard let cursor = mongoc_collection_aggregate(
			self._collection, MONGOC_QUERY_NONE, pipeline.data, opts?.data, nil) else {
			throw MongoError.invalidResponse()
		}
		guard let client = self._client else {
			throw MongoError.invalidClient()
		}
		return MongoCursor(fromCursor: cursor, withClient: client)
	}

	/**
	 * Counts the number of documents in this collection matching the provided filter.
	 *
	 * - Parameters:
	 *   - filter: a `Document`, the filter that documents must match in order to be counted
	 *   - options: Optional `CountOptions` to use when executing the command
	 *
	 * - Returns: The count of the documents that matched the filter
	 */
	public func count(_ filter: Document = [:], options: CountOptions? = nil) throws -> Int {
		let opts = try BsonEncoder().encode(options)
		var error = bson_error_t()
		// because we already encode skip and limit in the options,
		// pass in 0s so we don't get duplicate parameter errors.
		let count = mongoc_collection_count_with_opts(
			self._collection, MONGOC_QUERY_NONE, filter.data, 0, 0, opts?.data, nil, &error)

		if count == -1 { throw MongoError.commandError(message: toErrorString(error)) }

		return Int(count)
	}

	/**
	 * Finds the distinct values for a specified field across the collection.
	 *
	 * - Parameters:
	 *   - fieldName: The field for which the distinct values will be found
	 *   - filter: a `Document` representing the filter that documents must match in order to be considered for this operation
	 *   - options: Optional `DistinctOptions` to use when executing the command
	 *
	 * - Returns: A 'MongoCursor' containing the distinct values for the specified criteria
	 */
	public func distinct(fieldName: String, filter: Document = [:],
						 options: DistinctOptions? = nil) throws -> MongoCursor<Document> {
		guard let client = self._client else {
			throw MongoError.invalidClient()
		}

		let collName = String(cString: mongoc_collection_get_name(self._collection))
		let command: Document = [
			"distinct": collName,
			"key": fieldName,
			"query": filter
		]

		let opts = try BsonEncoder().encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_read_command_with_opts(
			self._collection, command.data, nil, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}

		let fakeReply: Document = [
			"ok": 1,
			"cursor": [
				"id": 0,
				"ns": "",
				"firstBatch": [reply]
			] as Document
		]

		// mongoc_cursor_new_from_command_reply will bson_destroy the data we pass in,
		// so copy it to avoid destroying twice (already done in Document deinit)
		let fakeData = bson_copy(fakeReply.data)
		guard let newCursor = mongoc_cursor_new_from_command_reply(client._client, fakeData, 0) else {
			throw MongoError.invalidResponse()
		}

		return MongoCursor(fromCursor: newCursor, withClient: client)
	}

	/**
	 * Encodes the provided value to BSON and inserts it. If the value is missing an identifier, one will be
	 * generated for it.
	 *
	 * - Parameters:
	 *   - value: A `CollectionType` value to encode and insert
	 *   - options: Optional `InsertOneOptions` to use when executing the command
	 *
	 * - Returns: The optional result of attempting to perform the insert. If the `WriteConcern`
	 *            is unacknowledged, `nil` is returned.
	 */
	@discardableResult
	public func insertOne(_ value: CollectionType, options: InsertOneOptions? = nil) throws -> InsertOneResult? {
		let encoder = BsonEncoder()
		let document = try encoder.encode(value)
		if document["_id"] == nil {
			try ObjectId().encode(to: document.data, forKey: "_id")
		}
		let opts = try encoder.encode(options)
		var error = bson_error_t()
		if !mongoc_collection_insert_one(self._collection, document.data, opts?.data, nil, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return InsertOneResult(insertedId: document["_id"]!)
	}

	/**
	 * Encodes the provided values to BSON and inserts them. If any values are missing identifiers,
	 * the driver will generate them.
	 *
	 * - Parameters:
	 *   - documents: The `CollectionType` values to insert
	 *   - options: Optional `InsertManyOptions` to use when executing the command
	 *
	 * - Returns: The optional result of attempting to performing the insert. If the write concern
	 *            is unacknowledged, nil is returned
	 */
	@discardableResult
	public func insertMany(_ values: [CollectionType], options: InsertManyOptions? = nil) throws -> InsertManyResult? {
		let encoder = BsonEncoder()

		let documents = try values.map { try encoder.encode($0) }
		for doc in documents where doc["_id"] == nil {
			try ObjectId().encode(to: doc.data, forKey: "_id")
		}
		var docPointers = documents.map { UnsafePointer($0.data) }

		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_insert_many(
			self._collection, &docPointers, documents.count, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return InsertManyResult(fromArray: documents.map { $0["_id"]! })
	}

	/**
	 * Replaces a single document matching the provided filter in this collection.
	 *
	 * - Parameters:
	 *   - filter: A `Document` representing the match criteria
	 *   - replacement: The replacement value, a `CollectionType` value to be encoded and inserted
	 *   - options: Optional `ReplaceOptions` to use when executing the command
	 *
	 * - Returns: The optional result of attempting to replace a document. If the `WriteConcern`
	 *            is unacknowledged, `nil` is returned.
	 */
	@discardableResult
	public func replaceOne(filter: Document, replacement: CollectionType, options: ReplaceOptions? = nil) throws -> UpdateResult? {
		let encoder = BsonEncoder()
		let replacementDoc = try encoder.encode(replacement)
		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_replace_one(
			self._collection, filter.data, replacementDoc.data, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return UpdateResult(from: reply)
	}

	/**
	 * Updates a single document matching the provided filter in this collection.
	 *
	 * - Parameters:
	 *   - filter: A `Document` representing the match criteria
	 *   - update: A `Document` representing the update to be applied to a matching document
	 *   - options: Optional `UpdateOptions` to use when executing the command
	 *
	 * - Returns: The optional result of attempting to update a document. If the `WriteConcern` is
	 *            unacknowledged, `nil` is returned.
	 */
	@discardableResult
	public func updateOne(filter: Document, update: Document, options: UpdateOptions? = nil) throws -> UpdateResult? {
		let encoder = BsonEncoder()
		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_update_one(
			self._collection, filter.data, update.data, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return UpdateResult(from: reply)
	}

	/**
	 * Updates multiple documents matching the provided filter in this collection.
	 *
	 * - Parameters:
	 *   - filter: A `Document` representing the match criteria
	 *   - update: A `Document` representing the update to be applied to matching documents
	 *   - options: Optional `UpdateOptions` to use when executing the command
	 *
	 * - Returns: The optional result of attempting to update multiple documents. If the write
	 *            concern is unacknowledged, nil is returned
	 */
	@discardableResult
	public func updateMany(filter: Document, update: Document, options: UpdateOptions? = nil) throws -> UpdateResult? {
		let encoder = BsonEncoder()
		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_update_many(
			self._collection, filter.data, update.data, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return UpdateResult(from: reply)
	}

	/**
	 * Deletes a single matching document from the collection.
	 *
	 * - Parameters:
	 *   - filter: A `Document` representing the match criteria
	 *   - options: Optional `UpdateOptions` to use when executing the command
	 *
	 * - Returns: The optional result of performing the deletion. If the `WriteConcern` is
	 *            unacknowledged, `nil` is returned.
	 */
	@discardableResult
	public func deleteOne(_ filter: Document, options: DeleteOptions? = nil) throws -> DeleteResult? {
		let encoder = BsonEncoder()
		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_delete_one(
			self._collection, filter.data, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return DeleteResult(from: reply)
	}

	/**
	 * Deletes multiple documents
	 *
	 * - Parameters:
	 *   - filter: Document representing the match criteria
	 *   - options: Optional `UpdateOptions` to use when executing the command
	 *
	 * - Returns: The optional result of performing the deletion. If the `WriteConcern` is
	 *            unacknowledged, `nil` is returned.
	 */
	@discardableResult
	public func deleteMany(_ filter: Document, options: DeleteOptions? = nil) throws -> DeleteResult? {
		let encoder = BsonEncoder()
		let opts = try encoder.encode(options)
		let reply = Document()
		var error = bson_error_t()
		if !mongoc_collection_delete_many(
			self._collection, filter.data, opts?.data, reply.data, &error) {
			throw MongoError.commandError(message: toErrorString(error))
		}
		return DeleteResult(from: reply)
	}
}