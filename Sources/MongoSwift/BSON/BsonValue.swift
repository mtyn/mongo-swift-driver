import Foundation
import libbson

/// The possible types of BSON values and their corresponding integer values.
public enum BsonType: Int {
    /// An invalid type
    case invalid = 0,
    /// 64-bit binary floating point
    double,
    /// UTF-8 string
    string,
    /// BSON document
    document,
    /// Array
    array,
    /// Binary data
    binary,
    /// Undefined value - deprecated
    undefined,
    /// A MongoDB ObjectId. 
    /// - SeeAlso: https://docs.mongodb.com/manual/reference/method/ObjectId/
    objectId,
    /// A boolean
    boolean,
    /// UTC datetime, stored as UTC milliseconds since the Unix epoch
    dateTime,
    /// Null value
    null,
    /// A regular expression
    regularExpression,
    /// A database pointer - deprecated
    dbPointer,
    /// Javascript code
    javascript,
    /// A symbol - deprecated
    symbol,
    /// JavaScript code w/ scope
    javascriptWithScope,
    /// 32-bit integer
    int32,
    /// Special internal type used by MongoDB replication and sharding
    timestamp,
    /// 64-bit integer
    int64,
    /// 128-bit decimal floating point
    decimal128,
    /// Special type which compares lower than all other possible BSON element values
    minKey,
    /// Special type which compares higher than all other possible BSON element values
    maxKey
}

/// A protocol all types representing BsonTypes must implement.
public protocol BsonValue {
    /// The `BsonType` of this value.
    var bsonType: BsonType { get }

    /**
    * Given the `bson_t` backing a `Document`, appends this `BsonValue` to the end.
    *
    * - Parameters:
    *   - to: An `<UnsafeMutablePointer<bson_t>`, indicating the `bson_t` to append to.
    *   - forKey: A `String`, the key with which to store the value.
    */
    func encode(to storage: DocumentStorage, forKey key: String) throws

    /**
    * Given a BSON iterator where the next stored value is known to be
    * an array, converts the data into an array. Assumes that the caller
    * has verified the next value is an array.
    *
    * - Parameters:
    *   - bson: A `bson_iter_t`
    *
    * - Side effects:
    *   - bson is moved forward to the next value in the document
    *
    * - Returns: A `[BsonValue]` corresponding to the array
    */
    init(from iter: DocumentIterator) throws
}

/// An extension of `Array` to represent the BSON array type.
extension Array: BsonValue {
    public var bsonType: BsonType { return .array }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        // An array is just a document with keys '0', '1', etc. corresponding to indexes
        var arr = Document()
        for (i, v) in self.enumerated() { arr[String(i)] = v as? BsonValue }
        if !bson_append_array(storage.pointer, key, Int32(key.count), arr.data) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        var length: UInt32 = 0
        let array = UnsafeMutablePointer<UnsafePointer<UInt8>?>.allocate(capacity: 1)
        defer {
            array.deinitialize(count: 1)
            array.deallocate(capacity: 1)
        }
        bson_iter_array(&iter.iter, &length, array)

        // since an array is a nested object with keys '0', '1', etc.,
        // create a new Document using the array data so we can recursively parse
        guard let arrayData = bson_new_from_data(array.pointee, Int(length)) else {
            throw MongoError.commandError(message: "blah")
        }

        let arrayDoc = Document(fromPointer: arrayData)

        let values: [BsonValue?] = (0..<arrayDoc.count).map { arrayDoc[String($0)] }

        self = values as! Array
    }
}

/// A struct to represent the BSON Binary type.
public struct Binary: BsonValue, Equatable, Codable {

    public var bsonType: BsonType { return .binary }

    /// The binary data.
    public let data: Data

    /// The binary subtype for this data.
    public let subtype: UInt8

    /// Subtypes for BSON Binary values.
    public enum Subtype: UInt8 {
        /// Generic binary subtype
        case generic,
        /// A function
        function,
        /// Binary (old)
        binaryDeprecated,
        /// UUID (old)
        uuidDeprecated,
        /// UUID (RFC 4122)
        uuid,
        /// MD5
        md5,
        /// User defined
        userDefined = 0x80
    }

    /// Initializes a `Binary` instance from a `Data` object and a `UInt8` subtype.
    public init(data: Data, subtype: UInt8) {
        self.subtype = subtype
        self.data = data
    }

    /// Initializes a `Binary` instance from a `Data` object and a `Subtype`. 
    public init(data: Data, subtype: Subtype) {
        self.init(data: data, subtype: subtype.rawValue)
    }

    /// Initializes a `Binary` instance from a base64 `String` and a `UInt8` subtype.
    /// Throws an error if the base64 `String` is invalid.
    public init(base64: String, subtype: UInt8) throws {
        guard let dataObj = Data(base64Encoded: base64) else {
            throw MongoError.invalidArgument(message: "failed to create Data object from invalid base64 string \(base64)")
        }
        self.init(data: dataObj, subtype: subtype)
    }

    /// Initializes a `Binary` instance from a base64 `String` and a `Subtype`.
    /// Throws an error if the base64 `String` is invalid. 
    public init(base64: String, subtype: Subtype) throws {
        try self.init(base64: base64, subtype: subtype.rawValue)
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        let subtype = bson_subtype_t(UInt32(self.subtype))
        let length = self.data.count
        let byteArray = [UInt8](self.data)
        if !bson_append_binary(storage.pointer, key, Int32(key.count), subtype, byteArray, UInt32(length)) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        var subtype: bson_subtype_t = bson_subtype_t(rawValue: 0)
        var length: UInt32 = 0
        let dataPointer = UnsafeMutablePointer<UnsafePointer<UInt8>?>.allocate(capacity: 1)
        defer {
            dataPointer.deinitialize(count: 1)
            dataPointer.deallocate(capacity: 1)
        }
        bson_iter_binary(&iter.iter, &subtype, &length, dataPointer)

        guard let data = dataPointer.pointee else {
            preconditionFailure("failed to retrieve data stored for binary BSON value")
        }

        let dataObj = Data(bytes: data, count: Int(length))
        self = Binary(data: dataObj, subtype: UInt8(subtype.rawValue))
    }

    public static func == (lhs: Binary, rhs: Binary) -> Bool {
        return lhs.data == rhs.data && lhs.subtype == rhs.subtype
    }
}

/// An extension of `Bool` to represent the BSON Boolean type.
extension Bool: BsonValue {
    public var bsonType: BsonType { return .boolean }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_bool(storage.pointer, key, Int32(key.count), self) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        self = bson_iter_bool(&iter.iter)
    }  
}

/// An extension of `Date` to represent the BSON Datetime type.
extension Date: BsonValue {
    public var bsonType: BsonType { return .dateTime }

    /// Initializes a new `Date` representing the instance `msSinceEpoch` milliseconds
    /// since the Unix epoch.
    public init(msSinceEpoch: Int64) {
        self.init(timeIntervalSince1970: Double(msSinceEpoch / 1000))
    }

    /// The number of milliseconds after the Unix epoch that this `Date` occurs.
    public var msSinceEpoch: Int64 { return Int64(self.timeIntervalSince1970 * 1000) }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        let seconds = self.timeIntervalSince1970 * 1000
        if !bson_append_date_time(storage.pointer, key, Int32(key.count), Int64(seconds)) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        self = Date(msSinceEpoch: bson_iter_date_time(&iter.iter))
    } 
}

/// An internal struct to represent the deprecated DBPointer type. While DBPointers cannot
/// be created, we may need to parse them into `Document`s, and this provides a place for that logic.
internal struct DBPointer: BsonValue {

    public var bsonType: BsonType { return .dbPointer }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        throw MongoError.bsonEncodeError(message: "DBPointers are deprecated; use a DBRef instead")
    }

    public init(from iter: DocumentIterator) throws {
        throw MongoError.commandError(message: "unimplemnented")
    }

    internal static func asDocument(from iter: DocumentIterator) throws -> Document {
        var length: UInt32 = 0
        let collectionPP = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocate(capacity: 1)
        defer {
            collectionPP.deinitialize(count: 1)
            collectionPP.deallocate(capacity: 1)
        }
        let oidPP = UnsafeMutablePointer<UnsafePointer<bson_oid_t>?>.allocate(capacity: 1)
        defer {
            oidPP.deinitialize(count: 1)
            oidPP.deallocate(capacity: 1)
        }
        bson_iter_dbpointer(&iter.iter, &length, collectionPP, oidPP)

        guard let oidP = oidPP.pointee else {
            preconditionFailure(retrieveErrorMsg(type: "DBPointer ObjectId", key: iter.currentKey))
        }
        guard let collectionP = collectionPP.pointee else {
            preconditionFailure(retrieveErrorMsg(type: "DBPointer collection name", key: iter.currentKey))
        }

        let dbRef: Document = [
            "$ref": String(cString: collectionP),
            "$id": ObjectId(fromPointer: oidP)
        ]

        return dbRef
    }
}

/// A struct to represent the BSON Decimal128 type.
public struct Decimal128: BsonValue, Equatable, Codable {
    /// This number, represented as a `String`.
    public let data: String

    /// Initializes a `Decimal128` value from the provided `String`.
    public init(_ data: String) {
        self.data = data
    }

    public var bsonType: BsonType { return .decimal128 }

    public static func == (lhs: Decimal128, rhs: Decimal128) -> Bool {
        return lhs.data == rhs.data
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        var value: bson_decimal128_t = bson_decimal128_t()
        precondition(bson_decimal128_from_string(self.data, &value),
            "Failed to parse Decimal128 string \(self.data)")
        if !bson_append_decimal128(storage.pointer, key, Int32(key.count), &value) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

     public init(from iter: DocumentIterator) throws {
        var value: bson_decimal128_t = bson_decimal128_t()
        precondition(bson_iter_decimal128(&iter.iter, &value), "Failed to retrieve Decimal128 value")

        var str = Data(count: Int(BSON_DECIMAL128_STRING))
        self = Decimal128(str.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<Int8>) in
            bson_decimal128_to_string(&value, bytes)
            return String(cString: bytes)
        })
    }
}

/// An extension of `Double` to represent the BSON Double type.
extension Double: BsonValue {
    public var bsonType: BsonType { return .double }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_double(storage.pointer, key, Int32(key.count), self) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        self = bson_iter_double(&iter.iter)
    }
}

/// An extension of `Int` to represent the BSON Int32 or Int64 type.
/// The `Int` will be encoded as an Int32 if possible, or an Int64 if necessary.
extension Int: BsonValue {
    public var bsonType: BsonType { return .int32 }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if let int32 = Int32(exactly: self) {
            return try int32.encode(to: storage, forKey: key)
        }
        if let int64 = Int64(exactly: self) {
            return try int64.encode(to: storage, forKey: key)
        }
        throw MongoError.bsonEncodeError(message: "`Int` value \(self) could not be encoded as `Int32` or `Int64`")
    }

    public init(from iter: DocumentIterator) throws {
        self = Int(bson_iter_int32(&iter.iter))
    }
}

/// An extension of `Int32` to represent the BSON Int32 type.
extension Int32: BsonValue {
    public var bsonType: BsonType { return .int32 }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_int32(storage.pointer, key, Int32(key.count), self) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        self = bson_iter_int32(&iter.iter)
    }
}

/// An extension of `Int64` to represent the BSON Int64 type.
extension Int64: BsonValue {
    public var bsonType: BsonType { return .int64 }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_int64(storage.pointer, key, Int32(key.count), self) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        self = bson_iter_int64(&iter.iter)
    }
}

/// A struct to represent the BSON Code and CodeWithScope types.
public struct CodeWithScope: BsonValue, Equatable, Codable {
    /// A string containing Javascript code.
    public let code: String
    /// An optional scope `Document` containing a mapping of identifiers to values,
    /// representing the context in which `code` should be evaluated.
    public let scope: Document?

    public var bsonType: BsonType {
        if self.scope != nil { return .javascriptWithScope }
        return .javascript
    }

    /// Initializes a `CodeWithScope` with an optional scope value.
    public init(code: String, scope: Document? = nil) {
        self.code = code
        self.scope = scope
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if let s = self.scope {
            if !bson_append_code_with_scope(storage.pointer, key, Int32(key.count), self.code, s.data) {
                throw bsonEncodeError(value: self, forKey: key)
            }
        } else if !bson_append_code(storage.pointer, key, Int32(key.count), self.code) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        var length: UInt32 = 0

        if bson_iter_type(&iter.iter) == BSON_TYPE_CODE {
            let code = String(cString: bson_iter_code(&iter.iter, &length))
            self = CodeWithScope(code: code)
            return
        }

        var scopeLength: UInt32 = 0
        let scopePointer = UnsafeMutablePointer<UnsafePointer<UInt8>?>.allocate(capacity: 1)
        defer {
            scopePointer.deinitialize(count: 1)
            scopePointer.deallocate(capacity: 1)
        }
        let code = String(cString: bson_iter_codewscope(&iter.iter, &length, &scopeLength, scopePointer))
        guard let scopeData = bson_new_from_data(scopePointer.pointee, Int(scopeLength)) else {
            preconditionFailure("Failed to create a bson_t from scope data")
        }
        let scopeDoc = Document(fromPointer: scopeData)
        self = CodeWithScope(code: code, scope: scopeDoc)
    }

    public static func == (lhs: CodeWithScope, rhs: CodeWithScope) -> Bool {
        return lhs.code == rhs.code && lhs.scope == rhs.scope
    }
}

/// A struct to represent the BSON MaxKey type.
public struct MaxKey: BsonValue, Equatable, Codable {
    private var maxKey = 1

    public var bsonType: BsonType { return .maxKey }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_maxkey(storage.pointer, key, Int32(key.count)) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        // throw if not maxkey type?
        self = MaxKey()
    }

    public init() {}

    public static func == (lhs: MaxKey, rhs: MaxKey) -> Bool { return true }
}

/// A struct to represent the BSON MinKey type.
public struct MinKey: BsonValue, Equatable, Codable {
    private var minKey = 1

    public var bsonType: BsonType { return .minKey }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_minkey(storage.pointer, key, Int32(key.count)) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        // throw if not minkey type?
        self = MinKey()
    }

    public init() {}

    public static func == (lhs: MinKey, rhs: MinKey) -> Bool { return true }
}

/// A struct to represent the BSON ObjectId type.
public struct ObjectId: BsonValue, Equatable, CustomStringConvertible, Codable {

    public var bsonType: BsonType { return .objectId }

    /// This `ObjectId`'s data represented as a `String`.
    public let oid: String

    /// Initializes a new `ObjectId`.
    public init() {
        var oid_t = bson_oid_t()
        bson_oid_init(&oid_t, nil)
        self.init(fromPointer: &oid_t)
    }

    /// Initializes an `ObjectId` from the provided `String`.
    public init(fromString oid: String) {
        self.oid = oid
    }

    /// Initializes an `ObjectId` from an `UnsafePointer<bson_oid_t>` by copying the data
    /// from it to a `String`
    internal init(fromPointer oid_t: UnsafePointer<bson_oid_t>) {
        var str = Data(count: 25)
        self.oid = str.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<Int8>) in
            bson_oid_to_string(oid_t, bytes)
            return String(cString: bytes)
        }
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        // create a new bson_oid_t with self.oid
        var oid = bson_oid_t()
        bson_oid_init_from_string(&oid, self.oid)
        // encode the bson_oid_t to the bson_t
        if !bson_append_oid(storage.pointer, key, Int32(key.count), &oid) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        guard let oid = bson_iter_oid(&iter.iter) else {
            preconditionFailure("Failed to retrieve ObjectID value")
        }
        self = ObjectId(fromPointer: oid)
    }

    public var description: String {
        return self.oid
    }

    public static func == (lhs: ObjectId, rhs: ObjectId) -> Bool {
        return lhs.oid == rhs.oid
    }

}

// A mapping of regex option characters to their equivalent `NSRegularExpression` option.
// note that there is a BSON regexp option 'l' that `NSRegularExpression`
// doesn't support. The flag will be dropped if BSON containing it is parsed,
// and it will be ignored if passed into `optionsFromString`.
let regexOptsMap: [Character: NSRegularExpression.Options] = [
    "i": .caseInsensitive,
    "m": .anchorsMatchLines,
    "s": .dotMatchesLineSeparators,
    "u": .useUnicodeWordBoundaries,
    "x": .allowCommentsAndWhitespace
]

/// An extension of `NSRegularExpression` to support converting options to and from strings.
extension NSRegularExpression {

    /// Convert a string of options flags into an equivalent `NSRegularExpression.Options`
    static func optionsFromString(_ stringOptions: String) -> NSRegularExpression.Options {
        var optsObj: NSRegularExpression.Options = []
        for o in stringOptions {
            if let value = regexOptsMap[o] {
                 optsObj.update(with: value)
            }
        }
        return optsObj
    }

    /// Convert this instance's options object into an alphabetically-sorted string of characters
    public var stringOptions: String {
        var optsString = ""
        for (char, o) in regexOptsMap { if options.contains(o) { optsString += String(char) } }
        return String(optsString.sorted())
    }
}

/// A struct to represent a BSON regular expression.
struct RegularExpression: BsonValue, Equatable, Codable {

    public var bsonType: BsonType { return .regularExpression }

    /// The pattern for this regular expression.
    public let pattern: String
    /// A string containing options for this regular expression. 
    /// - SeeAlso: https://docs.mongodb.com/manual/reference/operator/query/regex/#op
    public let options: String

    /// Initializes a new `RegularExpression` with the provided pattern and options.
    public init(pattern: String, options: String) {
        self.pattern = pattern
        self.options = String(options.sorted())
    }

    /// Initializes a new `RegularExpression` with the pattern and options of the provided `NSRegularExpression`.
    public init(from regex: NSRegularExpression) {
        self.pattern = regex.pattern
        self.options = regex.stringOptions
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_regex(storage.pointer, key, Int32(key.count), self.pattern, self.options) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        let options = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocate(capacity: 1)
        defer {
            options.deinitialize(count: 1)
            options.deallocate(capacity: 1)
        }

        guard let pattern = bson_iter_regex(&iter.iter, options) else {
            preconditionFailure("Failed to retrieve regular expression pattern")
        }
        let patternString = String(cString: pattern)

        guard let stringOptions = options.pointee else {
            preconditionFailure("Failed to retrieve regular expression options")
        }
        let optionsString = String(cString: stringOptions)

        self = RegularExpression(pattern: patternString, options: optionsString)
    }

    /// Creates an `NSRegularExpression` with the pattern and options of this `RegularExpression`.
    /// Note: `NSRegularExpression` does not support the `l` locale dependence option, so it will
    // be omitted if set on this `RegularExpression`.
    public var nsRegularExpression: NSRegularExpression {
        let opts = NSRegularExpression.optionsFromString(self.options)
        do {
            return try NSRegularExpression(pattern: self.pattern, options: opts)
        } catch {
            preconditionFailure("Failed to initialize NSRegularExpression with " +
                "pattern '\(self.pattern)'' and options '\(self.options)'")
        }
    }

    /// Returns `true` if the two `RegularExpression`s have matching patterns and options, and `false` otherwise.
    public static func == (lhs: RegularExpression, rhs: RegularExpression) -> Bool {
        return lhs.pattern == rhs.pattern && lhs.options == rhs.options
    }
}

/// An extension of String to represent the BSON string type.
extension String: BsonValue {
    public var bsonType: BsonType { return .string }
    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_utf8(storage.pointer, key, Int32(key.count), self, Int32(self.count)) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public init(from iter: DocumentIterator) throws {
        var length: UInt32 = 0
        let value = bson_iter_utf8(&iter.iter, &length)
        guard let strValue = value else {
            preconditionFailure(retrieveErrorMsg(type: "UTF-8", key: iter.currentKey))
        }

        self = String(cString: strValue)
    }
}

/// An internal struct to represent the deprecated Symbol type. While Symbols cannot be
/// created, we may need to parse them into `String`s, and this provides a place for that logic.
internal struct Symbol: BsonValue {

    public var bsonType: BsonType { return .symbol }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        throw MongoError.bsonEncodeError(message: "Symbols are deprecated; use a string instead")
    }

    public init(from iter: DocumentIterator) throws {
        throw MongoError.commandError(message: "unimpe,ented")
    }

    internal static func asString(from iter: DocumentIterator) throws -> String {
        var length: UInt32 = 0
        guard let strValue = bson_iter_symbol(&iter.iter, &length) else {
            preconditionFailure(retrieveErrorMsg(type: "Symbol", key: iter.currentKey))
        }
        return String(cString: strValue)
    }
}

/// A struct to represent the BSON Timestamp type.
public struct Timestamp: BsonValue, Equatable, Codable {
    public var bsonType: BsonType { return .timestamp }

    /// A timestamp representing seconds since the Unix epoch.
    public let timestamp: UInt32
    /// An incrementing ordinal for operations within a given second.
    public let increment: UInt32

    /// Initializes a new  `Timestamp` with the provided `timestamp` and `increment` values.
    public init(timestamp: UInt32, inc: UInt32) {
        self.timestamp = timestamp
        self.increment = inc
    }

    /// Initializes a new  `Timestamp` with the provided `timestamp` and `increment` values. Assumes
    /// the values can successfully be converted to `UInt32`s without loss of precision.
    public init(timestamp: Int, inc: Int) {
        self.timestamp = UInt32(timestamp)
        self.increment = UInt32(inc)
    }

    public init(from iter: DocumentIterator) throws {
        var t: UInt32 = 0
        var i: UInt32 = 0
        bson_iter_timestamp(&iter.iter, &t, &i)
        self = Timestamp(timestamp: t, inc: i) 
    }

    public func encode(to storage: DocumentStorage, forKey key: String) throws {
        if !bson_append_timestamp(storage.pointer, key, Int32(key.count), self.timestamp, self.increment) {
            throw bsonEncodeError(value: self, forKey: key)
        }
    }

    public static func == (lhs: Timestamp, rhs: Timestamp) -> Bool {
        return lhs.timestamp == rhs.timestamp && lhs.increment == rhs.increment
    }

}

func retrieveErrorMsg(type: String, key: String) -> String {
    return "Failed to retrieve the \(type) value for key '\(key)'"
}
