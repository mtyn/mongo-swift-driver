/// Options classes for use with CRUD operations 

/// Options to use when executing an `aggregate` command on a `MongoCollection`.
public struct AggregateOptions: Encodable {
	/// Enables writing to temporary files. When set to true, aggregation stages
	/// can write data to the _tmp subdirectory in the dbPath directory.
	public let allowDiskUse: Bool?

	/// The number of `Document`s to return per batch.
	public let batchSize: Int32?

	/// If true, allows the write to opt-out of document level validation. This only applies
	/// when the $out stage is specified.
	public let bypassDocumentValidation: Bool?

	/// Specifies a collation.
	public let collation: Document?

	/// The maximum amount of time to allow the query to run.
	public let maxTimeMS: Int64?

	/// Enables users to specify an arbitrary string to help trace the operation through
	/// the database profiler, currentOp and logs. The default is to not send a value.
	public let comment: String?

	/// The index to use for the aggregation. The hint does not apply to $lookup and $graphLookup stages.
	// let hint: Optional<(String | Document)>

	/// A `ReadConcern` to use in read stages of this operation.
	public let readConcern: ReadConcern?

	/// A `WriteConcern` to use in `$out` stages of this operation.
	public let writeConcern: WriteConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(allowDiskUse: Bool? = nil, batchSize: Int32? = nil, bypassDocumentValidation: Bool? = nil,
				collation: Document? = nil, comment: String? = nil, maxTimeMS: Int64? = nil,
				readConcern: ReadConcern? = nil, writeConcern: WriteConcern? = nil) {
		self.allowDiskUse = allowDiskUse
		self.batchSize = batchSize
		self.bypassDocumentValidation = bypassDocumentValidation
		self.collation = collation
		self.comment = comment
		self.maxTimeMS = maxTimeMS
		self.readConcern = readConcern
		self.writeConcern = writeConcern
	}
}

/// Options to use when executing a `count` command on a `MongoCollection`.
public struct CountOptions: Encodable {
	/// Specifies a collation.
	public let collation: Document?

	/// The index to use.
	// let hint: Optional<(String | Document)>

	/// The maximum number of documents to count.
	public let limit: Int64?

	/// The maximum amount of time to allow the query to run.
	public let maxTimeMS: Int64?

	/// The number of documents to skip before counting.
	public let skip: Int64?

	/// A ReadConcern to use for this operation. 
	public let readConcern: ReadConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(collation: Document? = nil, limit: Int64? = nil, maxTimeMS: Int64? = nil,
				readConcern: ReadConcern? = nil, skip: Int64? = nil) {
		self.collation = collation
		self.limit = limit
		self.maxTimeMS = maxTimeMS
		self.readConcern = readConcern
		self.skip = skip
	}
}

/// Options to use when executing a `distinct` command on a `MongoCollection`.
public struct DistinctOptions: Encodable {
	/// Specifies a collation.
	public let collation: Document?

	/// The maximum amount of time to allow the query to run.
	public let maxTimeMS: Int64?

	/// A ReadConcern to use for this operation. 
	public let readConcern: ReadConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(collation: Document? = nil, maxTimeMS: Int64? = nil, readConcern: ReadConcern? = nil) {
		self.collation = collation
		self.maxTimeMS = maxTimeMS
		self.readConcern = readConcern
	}
}

/// The possible types of `MongoCursor` an operation can return.
public enum CursorType {
	/**
	 * The default value. A vast majority of cursors will be of this type.
	 */
	case nonTailable

	/**
	 * Tailable means the cursor is not closed when the last data is retrieved.
	 * Rather, the cursor marks the final object’s position. You can resume
	 * using the cursor later, from where it was located, if more data were
	 * received. Like any “latent cursor”, the cursor may become invalid at
	 * some point (CursorNotFound) – for example if the final object it
	 * references were deleted.
	 *
	 * - SeeAlso: https://docs.mongodb.com/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
	 */
	case tailable

	/**
	 * Combines the tailable option with awaitData, as defined below.
	 *
	 * Use with TailableCursor. If we are at the end of the data, block for a
	 * while rather than returning no data. After a timeout period, we do return
	 * as normal. The default is true.
	 *
	 * - SeeAlso: https://docs.mongodb.com/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
	 */
	case tailableAwait
}

/// Options to use when executing a `find` command on a `MongoCollection`.
public struct FindOptions: Encodable {
	/// Get partial results from a mongos if some shards are down (instead of throwing an error).
	public let allowPartialResults: Bool?

	/// The number of documents to return per batch.
	public let batchSize: Int32?

	/// Specifies a collation.
	public let collation: Document?

	/// Attaches a comment to the query.
	public let comment: String?

	/// Indicates the type of cursor to use. This value includes both the tailable and awaitData options.
	// commenting this out until we decide how to encode cursorType.
	// let cursorType: CursorType?

	/// The index to use.
	// let hint: Optional<(String | Document)>

	/// The maximum number of documents to return.
	public let limit: Int64?

	/// The exclusive upper bound for a specific index.
	public let max: Document?

	/// The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
	/// query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
	/// this option is ignored.
	public let maxAwaitTimeMS: Int64?

	/// Maximum number of documents or index keys to scan when executing the query.
	public let maxScan: Int64?

	/// The maximum amount of time to allow the query to run.
	public let maxTimeMS: Int64?

	/// The inclusive lower bound for a specific index.
	public let min: Document?

	/// The server normally times out idle cursors after an inactivity period (10 minutes)
	/// to prevent excess memory use. Set this option to prevent that.
	public let noCursorTimeout: Bool?

	/// Limits the fields to return for all matching documents.
	public let projection: Document?

	/// If true, returns only the index keys in the resulting documents.
	public let returnKey: Bool?

	/// Determines whether to return the record identifier for each document. If true, adds a field $recordId
	/// to the returned documents.
	public let showRecordId: Bool?

	/// The number of documents to skip before returning.
	public let skip: Int64?

	/// The order in which to return matching documents.
	public let sort: Document?

	/// A ReadConcern to use for this operation. 
	public let readConcern: ReadConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(allowPartialResults: Bool? = nil, batchSize: Int32? = nil, collation: Document? = nil,
				comment: String? = nil, limit: Int64? = nil, max: Document? = nil, maxAwaitTimeMS: Int64? = nil,
				maxScan: Int64? = nil, maxTimeMS: Int64? = nil, min: Document? = nil, noCursorTimeout: Bool? = nil,
				projection: Document? = nil, readConcern: ReadConcern? = nil, returnKey: Bool? = nil,
				showRecordId: Bool? = nil, skip: Int64? = nil, sort: Document? = nil) {
		self.allowPartialResults = allowPartialResults
		self.batchSize = batchSize
		self.collation = collation
		self.comment = comment
		self.limit = limit
		self.max = max
		self.maxAwaitTimeMS = maxAwaitTimeMS
		self.maxScan = maxScan
		self.maxTimeMS = maxTimeMS
		self.min = min
		self.noCursorTimeout = noCursorTimeout
		self.projection = projection
		self.readConcern = readConcern
		self.returnKey = returnKey
		self.showRecordId = showRecordId
		self.skip = skip
		self.sort = sort
	}
}

/// Options to use when executing an `insertOne` command on a `MongoCollection`.
public struct InsertOneOptions: Encodable {
	/// If true, allows the write to opt-out of document level validation.
	public let bypassDocumentValidation: Bool?

	/// An optional WriteConcern to use for the command.
	public let writeConcern: WriteConcern?

	/// Convenience initializer allowing bypassDocumentValidation to be omitted or optional
	public init(bypassDocumentValidation: Bool? = nil, writeConcern: WriteConcern? = nil) {
		self.bypassDocumentValidation = bypassDocumentValidation
		self.writeConcern = writeConcern
	}
}

/// Options to use when executing an `insertMany` command on a `MongoCollection`. 
public struct InsertManyOptions: Encodable {
	/// If true, allows the write to opt-out of document level validation.
	public let bypassDocumentValidation: Bool?

	/// If true, when an insert fails, return without performing the remaining
	/// writes. If false, when a write fails, continue with the remaining writes, if any.
	/// Defaults to true.
	public var ordered: Bool = true

	/// An optional WriteConcern to use for the command.
	public let writeConcern: WriteConcern?

	/// Convenience initializer allowing any/all parameters to be omitted or optional
	public init(bypassDocumentValidation: Bool? = nil, ordered: Bool? = true, writeConcern: WriteConcern? = nil) {
		self.bypassDocumentValidation = bypassDocumentValidation
		if let o = ordered { self.ordered = o }
		self.writeConcern = writeConcern
	}
}

/// Options to use when executing an `update` command on a `MongoCollection`. 
public struct UpdateOptions: Encodable {
	/// A set of filters specifying to which array elements an update should apply.
	public let arrayFilters: [Document]?

	/// If true, allows the write to opt-out of document level validation.
	public let bypassDocumentValidation: Bool?

	/// Specifies a collation.
	public let collation: Document?

	/// When true, creates a new document if no document matches the query.
	public let upsert: Bool?

	/// An optional WriteConcern to use for the command.
	public let writeConcern: WriteConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(arrayFilters: [Document]? = nil, bypassDocumentValidation: Bool? = nil, collation: Document? = nil,
				upsert: Bool? = nil, writeConcern: WriteConcern? = nil) {
		self.arrayFilters = arrayFilters
		self.bypassDocumentValidation = bypassDocumentValidation
		self.collation = collation
		self.upsert = upsert
		self.writeConcern = writeConcern
	}
}

/// Options to use when executing a `replace` command on a `MongoCollection`. 
public struct ReplaceOptions: Encodable {
	/// If true, allows the write to opt-out of document level validation.
	public let bypassDocumentValidation: Bool?

	/// Specifies a collation.
	public let collation: Document?

	/// When true, creates a new document if no document matches the query.
	public let upsert: Bool?

	/// An optional `WriteConcern` to use for the command.
	public let writeConcern: WriteConcern?

	/// Convenience initializer allowing any/all parameters to be optional
	public init(bypassDocumentValidation: Bool? = nil, collation: Document? = nil, upsert: Bool? = nil,
				writeConcern: WriteConcern? = nil) {
		self.bypassDocumentValidation = bypassDocumentValidation
		self.collation = collation
		self.upsert = upsert
		self.writeConcern = writeConcern
	}
}

/// Options to use when executing a `delete` command on a `MongoCollection`. 
public struct DeleteOptions: Encodable {
	/// Specifies a collation.
	public let collation: Document?

	/// An optional `WriteConcern` to use for the command.
	public let writeConcern: WriteConcern?

	 /// Convenience initializer allowing collation to be omitted or optional
	public init(collation: Document? = nil, writeConcern: WriteConcern? = nil) {
		self.collation = collation
		self.writeConcern = writeConcern
	}
}

/// The result of an `insertOne` command on a `MongoCollection`. 
public struct InsertOneResult {
	/// The identifier that was inserted. If the document doesn't have an identifier, this value
	/// will be generated and added to the document before insertion.
	public let insertedId: BsonValue
}

/// The result of an `insertMany` command on a `MongoCollection`. 
public struct InsertManyResult {
	/// Map of the index of the inserted document to the id of the inserted document.
	public let insertedIds: [Int64: BsonValue]

	/// Given an ordered array of insertedIds, creates a corresponding InsertManyResult.
	internal init(fromArray arr: [BsonValue]) {
		var inserted = [Int64: BsonValue]()
		for (i, id) in arr.enumerated() {
			let index = Int64(i)
			inserted[index] = id
		}
		self.insertedIds = inserted
	}
}

/// The result of a `delete` command on a `MongoCollection`. 
public struct DeleteResult {
	/// The number of documents that were deleted.
	public let deletedCount: Int

	/// Given a server response to a delete command, creates a corresponding
	/// `DeleteResult`. If the `from` Document does not have a `deletedCount`
	/// field, the initialization will fail.
	internal init?(from: Document) {
		guard let deletedCount = from["deletedCount"] as? Int else { return nil }
		self.deletedCount = deletedCount
	}
}

/// The result of an `update` operation a `MongoCollection`.
public struct UpdateResult {
	/// The number of documents that matched the filter.
	public let matchedCount: Int

	/// The number of documents that were modified.
	public let modifiedCount: Int

	/// The identifier of the inserted document if an upsert took place.
	public let upsertedId: BsonValue?

	/// Given a server response to an update command, creates a corresponding
	/// `UpdateResult`. If the `from` Document does not have `matchedCount` and
	/// `modifiedCount` fields, the initialization will fail. The document may
	/// optionally have an `upsertedId` field.
	internal init?(from: Document) {
		 guard let matched = from["matchedCount"] as? Int, let modified = from["modifiedCount"] as? Int else {
			return nil
		 }
		 self.matchedCount = matched
		 self.modifiedCount = modified
		 self.upsertedId = from["upsertedId"]
	}
}
