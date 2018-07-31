import Foundation
import libbson

/// The storage class backing `Document` structs.
public class DocumentStorage {
    internal var pointer: UnsafeMutablePointer<bson_t>!

    init() {
        self.pointer = bson_new()
    }

    init(fromPointer pointer: UnsafePointer<bson_t>) {
        self.pointer = bson_copy(pointer)
    }

    deinit {
        guard let pointer = self.pointer else { return }
        bson_destroy(pointer)
        self.pointer = nil
    }
}

/// A struct representing the BSON document type.
public struct Document: ExpressibleByDictionaryLiteral, ExpressibleByArrayLiteral {
    internal var storage: DocumentStorage

    internal var data: UnsafeMutablePointer<bson_t>! { return storage.pointer }

    /// Returns a `[String]` containing the keys in this `Document`.
    public var keys: [String] {
        return self.makeIterator().keys
    }

    /// Returns a `[BsonValue?]` containing the values stored in this `Document`.
    public var values: [BsonValue?] {
        return self.makeIterator().values
    }

    /// Returns the number of (key, value) pairs stored at the top level
    /// of this `Document`.
    public var count: Int { return Int(bson_count_keys(self.data)) }

    /// Initializes a new, empty `Document`.
    public init() {
        self.storage = DocumentStorage()
    }

    /**
     * Initializes a `Document` from a pointer to a bson_t. Uses a copy
     * of `bsonData`, so the caller is responsible for freeing the original
     * memory.
     *
     * - Parameters:
     *   - fromPointer: a UnsafePointer<bson_t>
     *
     * - Returns: a new `Document`
     */
    internal init(fromPointer pointer: UnsafePointer<bson_t>) {
        self.storage = DocumentStorage(fromPointer: pointer)
    }

    /**
     * Initializes a `Document` using a dictionary literal where the
     * keys are `String`s and the values are `BsonValue?`s. For example:
     * `d: Document = ["a" : 1 ]`
     *
     * - Parameters:
     *   - dictionaryLiteral: a [String: BsonValue?]
     *
     * - Returns: a new `Document`
     */
    public init(dictionaryLiteral doc: (String, BsonValue?)...) {
        self.storage = DocumentStorage()
        for (k, v) in doc {
            self[k] = v
        }
    }
    /**
     * Initializes a `Document` using an array literal where the values
     * are `BsonValue`s. Values are stored under a string of their
     * index in the array. For example:
     * `d: Document = ["a", "b"]` will become `["0": "a", "1": "b"]`
     *
     * - Parameters:
     *   - arrayLiteral: a `[BsonValue?]`
     *
     * - Returns: a new `Document`
     */
    public init(arrayLiteral elements: BsonValue?...) {
        self.init(elements)
    }

    /**
     * Initializes a `Document` using an array where the values are optional
     * `BsonValue`s. Values are stored under a string of their index in the
     * array.
     *
     * - Parameters:
     *   - elements: a `[BsonValue?]`
     *
     * - Returns: a new `Document`
     */
    internal init(_ elements: [BsonValue?]) {
        self.storage = DocumentStorage()
        for (i, elt) in elements.enumerated() {
            self[String(i)] = elt
        }
    }

    /**
     * Constructs a new `Document` from the provided JSON text
     *
     * - Parameters:
     *   - fromJSON: a JSON document as `Data` to parse into a `Document`
     *
     * - Returns: the parsed `Document`
     */
    public init(fromJSON: Data) throws {
        self.storage = DocumentStorage(fromPointer: try fromJSON.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) in
            var error = bson_error_t()
            guard let bson = bson_new_from_json(bytes, fromJSON.count, &error) else {
                throw MongoError.bsonParseError(
                    domain: error.domain,
                    code: error.code,
                    message: toErrorString(error)
                )
            }

            return UnsafePointer(bson)
        })
    }

    /// Convenience initializer for constructing a `Document` from a `String`
    public init(fromJSON json: String) throws {
        try self.init(fromJSON: json.data(using: .utf8)!)
    }

    /// Constructs a `Document` from raw BSON `Data`
    public init(fromBSON: Data) {
        self.storage = DocumentStorage(fromPointer: fromBSON.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) in
            return bson_new_from_data(bytes, fromBSON.count)
        })
    }

    /// Returns the relaxed extended JSON representation of this `Document`.
    /// On error, an empty string will be returned.
    public var extendedJSON: String {
        guard let json = bson_as_relaxed_extended_json(self.data, nil) else {
            return ""
        }

        return String(cString: json)
    }

    /// Returns the canonical extended JSON representation of this `Document`.
    /// On error, an empty string will be returned.
    public var canonicalExtendedJSON: String {
        guard let json = bson_as_canonical_extended_json(self.data, nil) else {
            return ""
        }

        return String(cString: json)
    }

    /// Returns a copy of the raw BSON data for this `Document`, represented as `Data`
    public var rawBSON: Data {
        let data = bson_get_data(self.data)
        let length = self.data.pointee.len
        return Data(bytes: data!, count: Int(length))
    }

    /**
     * Allows setting values and retrieving values using subscript syntax.
     * For example:
     *  ```
     *  let d = Document()
     *  d["a"] = 1
     *  print(d["a"]) // prints 1
     *  ```
     */
    public subscript(key: String) -> BsonValue? {
        get {
            guard let iter = DocumentIterator(forDocument: self, andFind: key) else { return nil }
            return iter.currentValue
        }

        set(newValue) {
            self.copyStorageIfRequired()

            guard let value = newValue else {
                if !bson_append_null(self.data, key, Int32(key.count)) {
                    preconditionFailure("Failed to set the value for key \(key) to null")
                }
                return
            }

            do {
                try value.encode(to: self.storage, forKey: key)
            } catch {
                preconditionFailure("Failed to set the value for key \(key) to \(value): \(error)")
            }

        }
    }

    /**
     * Allows retrieving and strongly typing a value at the same time. This means you can avoid
     * having to cast and unwrap values from the `Document` when you know what type they will be.
     * For example:
     * ```
     *  let d: Document = ["x": 1]
     *  let x: Int = try d.get("x")
     *  ```
     *
     *  - Params:
     *      - key: The key under which the value you are looking up is stored
     *      - `T`: Any type conforming to the `BsonValue` protocol
     *  - Returns:
     *      - The value stored under key, as type `T`
     *  - Throws:
     *      - A `MongoError.typeError` if the value cannot be cast to type `T` or is not in the `Document`
     *
     */
    public func get<T: BsonValue>(_ key: String) throws -> T {
        guard let value = self[key] as? T else {
            throw MongoError.typeError(message: "Could not cast value for key \(key) to type \(T.self)")
        }
        return value
    }

    /// Appends the key/value pairs from the provided `doc` to this `Document`. 
    public mutating func merge(_ doc: Document) throws {
        self.copyStorageIfRequired()
        if !bson_concat(self.data, doc.data) {
            throw MongoError.bsonEncodeError(message: "Failed to merge \(doc) with \(self)")
        }
    }

    /// Checks if the document is uniquely referenced. If not, makes a copy
    /// of the underlying `bson_t` and lets the copy/copies keep the original.
    /// This allows us to provide value semantics for `Document`s. 
    /// This happens if someone copies a document and modifies it.
    /// For example: 
    ///  let doc1: Document = ["a": 1]
    ///  var doc2 = doc1
    ///  doc2["b"] = 2
    /// Therefore, this function should be called just before we are about to
    /// modify a document - either by setting a value or merging in another doc.
    private mutating func copyStorageIfRequired() {
        if !isKnownUniquelyReferenced(&self.storage) {
            self.storage = DocumentStorage(fromPointer: self.data)
        }
    }
}

/// An extension of `Document` to make it a `BsonValue`.
extension Document: BsonValue {
    public var bsonType: BsonType { return .document }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_document(storage.pointer, key, Int32(key.count), self.data) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        var length: UInt32 = 0
        let document = UnsafeMutablePointer<UnsafePointer<UInt8>?>.allocate(capacity: 1)
        defer {
            document.deinitialize(count: 1)
            document.deallocate(capacity: 1)
        }

        bson_iter_document(&iter.iter, &length, document)

        guard let docData = bson_new_from_data(document.pointee, Int(length)) else {
            preconditionFailure("Failed to create a bson_t from document data")
        }

        self = Document(fromPointer: docData)
    }

}

/// An extension of `Document` to make it `Equatable`.
extension Document: Equatable {
    public static func == (lhs: Document, rhs: Document) -> Bool {
        return bson_compare(lhs.data, rhs.data) == 0
    }
}

/// An extension of `Document` to make it convertible to a string.
extension Document: CustomStringConvertible {
    /// Returns the relaxed extended JSON representation of this `Document`.
    /// On error, an empty string will be returned.
    public var description: String {
        return self.extendedJSON
    }
}

/// An extension of `Document` to make it conform to the `Sequence` protocol.
/// This allows you to iterate through the (key, value) pairs, for example:
/// ```
/// let doc: Document = ["a": 1, "b": 2]
/// for (key, value) in doc {
///     ...
/// }
/// ```
extension Document: Sequence {
    /// Returns a `DocumentIterator` over the values in this `Document`. 
    public func makeIterator() -> DocumentIterator {
        return Iterator(forDocument: self)
    }
}

/// An iterator over the values in a `Document`. 
public class DocumentIterator: IteratorProtocol {
    // must be a var because we use it as an inout argument
    internal var iter: bson_iter_t

    /**
     * Initializes a new iterator over the provided document. The document's backing `DocumentStorage`
     * must remain valid for the lifetime of the iterator, and modifying the `Document` while using the
     * iterator is an error.
     * 
     * - Params:
     *   - doc: the `Document` to traverse
     */
    internal init(forDocument doc: Document) {
        self.iter = bson_iter_t()
        if !bson_iter_init(&self.iter, doc.data) {
            preconditionFailure("Failed to initialize an iterator for document \(doc)")
        }
    }

    /**
     * Initializes a new iterator over the provided document and advances it to the specified
     * key. Returns nil if the iterator cannot be initialized or if the key is not found.
     * The document's backing `DocumentStorage` must remain valid for the lifetime of the iterator,
     * and modifying the `Document` while using the iterator is an error.
     * 
     * - Params:
     *   - doc: the `Document` to traverse
     *   - key: the `String` key to advance the iterator to
     */
    internal init?(forDocument doc: Document, andFind key: String) {
        self.iter = bson_iter_t()
        if !bson_iter_init_find(&iter, doc.data, key.cString(using: .utf8)) { return nil }
    }

    /// Advances the iterator to the next value. 
    /// Returns true if the iterator was successfully advanced.
    /// Returns false if the end of the document was reached or invalid BSON data was encountered.
    private func advance() -> Bool {
        return bson_iter_next(&self.iter)
    }

    /// Advances the iterator to the end and returns a list of the keys traversed.
    internal var keys: [String] {
        var keys = [String]()
        while self.advance() {
            keys.append(self.currentKey)
        }
        return keys
    }

    /// Advances the iterator to the end and returns a list of the values traversed.
    internal var values: [BsonValue?] {
        var values = [BsonValue?]()
        while self.advance() {
            values.append(self.currentValue)
        }
        return values
    }

    /// Returns the key for the element the iterator is currently on. Assumes that the
    /// iterator is at a valid location, i.e. the latest call to `advance` returned true.
    internal var currentKey: String {
        guard let key = bson_iter_key(&self.iter) else {
            preconditionFailure("Failed to retrieve key for value with BSON type \(self.currentType)")
        }
        return String(cString: key)
    }

    /// Returns the value for the element the iterator is currently on. Assumes that the
    /// iterator is at a valid location, i.e. the latest call to `advance` returned true.
    internal var currentValue: BsonValue? {
        // note: encountering an unknown BSON type will result in returning nil here.
        guard let typeToReturn = BsonTypeMap[self.currentType] else { return nil }

        do {
            switch typeToReturn {
            case is Symbol.Type:
                return try Symbol.asString(from: self)
            case is DBPointer.Type:
                return try DBPointer.asDocument(from: self)
            default:
                return try typeToReturn.init(from: self)
            }
        } catch {
            preconditionFailure("Failed to initialize type \(typeToReturn): \(error)")
        }
    }

    /// Returns the type of the element the iterator is currently on. Assumes that the
    /// iterator is at a valid location, i.e. the latest call to `advance` returned true.
    internal var currentType: UInt32 {
        return bson_iter_type(&self.iter).rawValue
    }

    /// Returns the next value in the sequence, or `nil` if at the end.
    public func next() -> (String, BsonValue?)? {
        if self.advance() {
            return (self.currentKey, self.currentValue)
        }
        return nil
    }
}

internal let BsonTypeMap: [UInt32: BsonValue.Type] = [
    0x01: Double.self,
    0x02: String.self,
    0x03: Document.self,
    0x04: [BsonValue?].self,
    0x05: Binary.self,
    0x07: ObjectId.self,
    0x08: Bool.self,
    0x09: Date.self,
    0x0b: RegularExpression.self,
    0x0c: DBPointer.self,
    0x0d: CodeWithScope.self,
    0x0e: Symbol.self,
    0x0f: CodeWithScope.self,
    0x10: Int.self,
    0x11: Timestamp.self,
    0x12: Int64.self,
    0x13: Decimal128.self,
    0xff: MinKey.self,
    0x7f: MaxKey.self
]
