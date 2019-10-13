/**
    DataFrame.h
    Contains Classes: [Data, DataFrame]

    @author Jonathan Qassis
    @version 1.0 10/12/2019
*/

#ifndef DATASTORAGE_DATAFRAME_H
#define DATASTORAGE_DATAFRAME_H

// Dependencies
#include <unordered_map> // unordered_map
#include <unordered_set> // unordered_set
#include <vector> // vector
#include <map> // map
#include <set> // set
#include <string> // string
#include <stdexcept> // out_of_range
#include <fstream> // ifstream
#include <algorithm> // remove_if
#include <boost/date_time.hpp> // ptime
#include <boost/tokenizer.hpp> // Tokenizer

namespace bpt = boost::posix_time;

/**
    Returns true if the specified character is inside the given range.

    @param c The character to check.
    @return True if the character is in the given range, false otherwise.
*/
static std::function<bool(unsigned char)> invalidCharLambda = [](unsigned char c){
    return !(c >= 32 && c < 127);
};

/**
    Data
    This class manages data in association with assets and features.
*/
template <typename T>
class Data{
//private:
    // shorthand for unordered_map iterator
    using iterator = typename std::unordered_map<std::string,
        std::unordered_map<std::string, T>>::iterator;
    // shorthand for unordered_map const_iterator
    using const_iterator = typename std::unordered_map<std::string,
        std::unordered_map<std::string, T>>::const_iterator;

    // visual representation would look like: {asset : {features: data of type T }}
    std::unordered_map<std::string, std::unordered_map<std::string, T>> data;

public:
    /**
        Default constructor
        Creates an empty Data object.
    */
    Data() noexcept;

    /**
        Copy constructor
        Copies all data from lvalue to a new Data object.

        @param lvalue Data object to copy content from.
    */
    Data(const Data<T>& lvalue) noexcept;

    /**
        Move constructor
        Moves all data from rvalue to a new Data object.

        @param rvalue Data object to move content from.
    */
    Data(Data<T>&& rvalue) noexcept;

    /**
        Copy assignment operator
        Copies all data from lvalue to a new Data object.

        @param lvalue Data object to copy content from.
        @return new Data object with copied content from lvalue.
    */
    Data<T>& operator=(const Data<T>& lvalue) noexcept;

    /**
        Move assignment operator
        Moves all data from rvalue to a new Data object.

        @param rvalue Data object to move content from.
        @return new Data object with moved content from rvalue.
    */
    Data<T>& operator=(Data<T>&& rvalue) noexcept;

    /**
        Stream Operator
        Meant to write this object in human readable format to ostream os.

        @param os output stream to write to.
        @param df Data object to be written to os.
        @returns ostream os.
    */
    friend std::ostream& operator<<(std::ostream& os, const Data<T>& df) noexcept {
        df.toString(os);
        return os;
    }

    /**
        Returns the number of assets this object holds (number of keys in data).

        @return data.size().
    */
    size_t size() const noexcept { return data.size(); }

    /**
        Returns true if there are entries in this Data object, false otherwise.

        @return data.empty().
    */
    bool empty() const noexcept { return data.empty(); }

    /**
        An iterator referring to the first element of the container, or if the container
        is empty the past-the-end value for the container.

        @return data.begin().
    */
    iterator begin() noexcept { return data.begin(); }

    /**
        A constant iterator referring to the first element of the container, or if the
        container is empty the past-the-end value for the container.

        @return data.cbegin().
    */
    const_iterator cbegin() const noexcept { return data.cbegin(); }

    /**
        An iterator which refers to the past-the-end value for the container.

        @return data.end().
    */
    iterator end() noexcept { return data.end(); }

    /**
        A constant iterator which refers to the past-the-end value for the container.

        @return data.cend().
    */
    const_iterator cend() const noexcept { return data.cend(); }

    /**
        Sets the data for a given asset that refers to a given feature and value
        if and only if there is no entry with the given asset and feature.

        @param asset The asset which will holds feature and value data.
        @param feature The feature which will be reference by the asset.
        @param val The value of the feature being inserted.
    */
    void setData(const std::string& asset, const std::string& feature, const T& val) noexcept;

    /**
        Returns the value associated with the asset and feature given if and only if
        the asset and feature exist as an entry in this Data object, otherwise return
        default value of type T.

        @param asset The asset in which we want to find the value of feature.
        @param feature The feature we want the value of.
        @return Data of type T for given asset and feature.
    */
    T getData(const std::string& asset, const std::string& feature) const noexcept;

    /**
        toString method allows you to turn this object into a human readable format and
        write it to the param os.

        @param os output stream to write to.
    */
    void toString(std::ostream& os) const noexcept;
};

/**
    DataFrame
    This class manages csv files allowing for iteration of data. The rows do not need to be
    inorder but are guaranteed to be iterated in order after insertion.
    The first row (header) of the csv is used as features except for the first column of the
    first row which is completely ignored. Every subsequent row of the csv has it's first
    column as a date and all subsequent columns as values for each feature.

    Example acceptable csv: from top left to bottom right.
    | Ignored Column | Feature1        | Feature2        | Feature        | Feature        |
    | Date1          | feat1_date1_val | feat2_date1_val | feat3_date1_val | feat4_date1_val |
    | Date2          | feat1_date2_val | feat2_date2_val | feat3_date2_val | feat4_date2_val |
    | ...            | feat1_..._val   | feat2_..._val   | feat3_..._val   | feat4_..._val   |
    | DateN          | feat1_dateN_val | feat2_dateN_val | feat3_dateN_val | feat4_dateN_val |

    Typical use looks like:
    DataFrame dataframe;
    dataframe.fromCSV("EUR_USD", "path/to/file/EUR_USD.csv");
*/

template <typename T> // data type T
class DataFrame{
//private:
    // shorthand for map iterator
    using iterator = typename std::map<bpt::ptime, Data<T>>::iterator;
    // shorthand for map reverse_iterator
    using reverse_iterator = typename std::map<bpt::ptime, Data<T>>::reverse_iterator;
    // shorthand for map const_iterator
    using const_iterator = typename std::map<bpt::ptime, Data<T>>::const_iterator;
    // shorthand for map const_reverse_iterator
    using const_reverse_iterator = typename std::map<bpt::ptime, Data<T>>::const_reverse_iterator;

    // formats for parsing date and time data
    std::vector<std::locale> formats = {
        std::locale(std::locale::classic(), new bpt::time_input_facet("%Y-%m-%d")),
        std::locale(std::locale::classic(), new bpt::time_input_facet("%Y-%m-%d %H:%M")),
        std::locale(std::locale::classic(), new bpt::time_input_facet("%Y-%m-%d %H:%M:%S"))};
    // all assets to their features
    std::unordered_map<std::string, std::unordered_set<std::string>> assetsToFeatures;
    // date and time value to the Data object containing information for its given key
    std::map<bpt::ptime, Data<T>> data;

    /**
        Returns the template representation T of the string str.

        @param str The string to be converted into type T.
        @return The representation of str as type T.
    */
    inline T convert(const std::string& str) const noexcept;

    /**
        Converts each element [0, ..., N] in rowData into type T then inserts it into dataObj
        for asset and for each feature [0, ..., N] in features.

        @param dataObj Data object to insert data into.
        @param asset String for all features to be associated with.
        @param features The features to be associated with asset.
        @param rowData The data for each feature.
    */
    inline void insertData(Data<T>& dataObj, const std::string& asset,
        const std::vector<std::string>& features, const std::vector<std::string>& rowData) noexcept;

public:
    /**
        Default constructor
        Creates an empty DataFrame object.
    */
    DataFrame() noexcept;

    /**
        Copy constructor
        Copies all data from lvalue to a new DataFrame object.

        @param lvalue DataFrame object to copy content from.
    */
    DataFrame(const DataFrame<T>& obj) noexcept;

    /**
        Move constructor
        Moves all data from rvalue to a new DataFrame object.

        @param rvalue DataFrame object to move content from.
    */
    DataFrame(DataFrame<T>&& obj) noexcept;

    /**
        Copy assignment operator
        Copies all data from lvalue to a new DataFrame object.

        @param lvalue DataFrame object to copy content from.
        @return new DataFrame object with copied content from lvalue.
    */
    DataFrame<T>& operator=(const DataFrame<T>& lvalue) noexcept;

    /**
        Move assignment operator
        Moves all data from rvalue to a new DataFrame object.

        @param rvalue DataFrame object to move content from.
        @return new DataFrame object with moved content from rvalue.
    */
    DataFrame<T>& operator=(DataFrame<T>&& rvalue) noexcept;

    /**
        Stream Operator
        Meant to write this object in human readable format to ostream os.

        @param os output stream to write to.
        @param df DataFrame object to be written to os.
        @returns ostream os.
    */
    friend std::ostream& operator<<(std::ostream& os, const DataFrame<T>& df) noexcept {
        df.toString(os);
        return os;
    }

    /**
        Returns the number of date times (ptime) to Data objects this object holds.

        @return data.size().
    */
    size_t size() const noexcept { return data.size(); }

    /**
        Returns true if there are entries in this DataFrame object, false otherwise.

        @return data.empty().
    */
    bool empty() const noexcept { return data.empty(); }

    /**
        An iterator referring to the first element of the container, or if the container
        is empty the past-the-end value for the container.

        @return data.begin().
    */
    iterator begin() noexcept { return data.begin(); }

    /**
        An iterator referring to the last element of the container, or if the container
        is empty the reverse past-the-end value for the container.

        @return data.rbegin().
    */
    reverse_iterator rbegin() noexcept { return data.rbegin(); }

    /**
        A constant iterator referring to the first element of the container, or if the
        container is empty the past-the-end value for the container.

        @return data.cbegin().
    */
    const_iterator cbegin() const noexcept { return data.cbegin(); }

    /**
        A constant iterator referring to the last element of the container, or if the
        container is empty the reverse past-the-end value for the container.

        @return data.crbegin().
    */
    const_reverse_iterator crbegin() const noexcept { return data.crbegin(); }

    /**
        An iterator which refers to the past-the-end value for the container.

        @return data.end().
    */
    iterator end() noexcept { return data.end(); }

    /**
        An iterator which refers to the reverse past-the-end value for the container.

        @return data.rend().
    */
    reverse_iterator rend() noexcept { return data.rend(); }

    /**
        A constant iterator which refers to the past-the-end value for the container.

        @return data.cend().
    */
    const_iterator cend() const noexcept { return data.cend(); }

    /**
        A constant iterator which refers to the reverse past-the-end value for the container.

        @return data.crbegin().
    */
    const_reverse_iterator crend() const noexcept { return data.crend(); }

    /**
        Return whether or not this DataFrame object contains a given asset.

        @param asset String to check for.
        @return Return True if this DataFrame object contains the asset, false otherwise.
    */
    bool containsAsset(const std::string& asset) const noexcept;

    /**
        Return whether or not this DataFrame object contains a given date.

        @param date ptime to check for.
        @return Return True if this DataFrame object contains the date, false otherwise.
    */
    bool containsDate(const bpt::ptime& date) const noexcept;

    /**
        Return a mapping of assets to their features.

        @return Return std::unordered_map<std::string, std::unordered_set<std::string>>
    */
    const std::unordered_map<std::string, std::unordered_set<std::string>>&
        getAssetAndFeatures() const noexcept;

    /**
        Add a new format to parse date and times by, if you had a unique format in your
        csv not already considered.
        Example: "%Y-%m-%d %H:%M:%S"

        @param The string representation of the format to be added as a possible parsing.
    */
    void addDateFormat(const std::string& format) noexcept;

    /**
        Adds a new formats to parse date and times by, if you had a unique format in your
        csv not already considered.
        Example: {"%Y-%m-%d %H:%M:%S"}

        @param A vector of string representations of the formats to be added as a possible parsing.
    */
    void addDateFormat(const std::vector<std::string>& format) noexcept;

    /**
        Insert all data from the csv into this DataFrame Object. The filename will act as the asset.

        @param path String to the file csv to parse.
    */
    void fromCSV(const std::string& path) noexcept;

    /**
        Insert all data from the csv into this DataFrame Object.

        @param asset Asset name representing this file.
        @param path String to the file csv to parse.
    */
    void fromCSV(const std::string& asset, const std::string& path) noexcept;

    /**
        Removes all date entries that don't have any data associated with them.
    */
    void removeEmptyDates() noexcept;

    /**
        Add ptime to this DataFrame object based on a time period.
        Example: given a time period of day, add in a ptime for every day between begin() and end().

        @todo Implement the function logic.
    */
    void fillInGaps() noexcept;

    /**
        Return the associated data for a given date, asset, and feature if and only if the
        date, asset, and feature exist, otherwise return the default empty value of type T.

        @param date ptime to search in for asset and feature.
        @param asset String to search in for feature.
        @param feature to find the data value of type T for.
    */
    T getData(const bpt::ptime& date, const std::string& asset, const std::string& feature) const noexcept;

    /**
        toString method allows you to turn this object into a human readable format and
        write it to the param os.

        @param os output stream to write to.
    */
    void toString(std::ostream& os) const noexcept;

    /**
        Returns a string representing the date given.

        @param date The ptime to turn into a string format.
        @return The string representation of date.
    */
    static std::string getDate(const bpt::ptime& date) noexcept;

    /**
        Returns an integer representing the day of the week for the given date.
        [0 = Sunday, 1 = Monday, ..., 6 = Saturday]

        @param date The date to get the day of the week.
        @return The integer value representing the day
    */
    static int getDayOfWeek(const bpt::ptime& date) noexcept;
};

/*************************************************************************************************/
/*************************************** Data Definition *****************************************/
/*************************************************************************************************/
// Default constructor
template <typename T>
Data<T>::Data() noexcept {}

// Copy constructor
template <typename T>
Data<T>::Data(const Data<T>& lvalue) noexcept
: data(lvalue.data) {}

// Move constructor
template <typename T>
Data<T>::Data(Data<T>&& rvalue) noexcept
: data(std::move(rvalue.data)) {}

// Copy assignment operator
template <typename T>
Data<T>& Data<T>::operator=(const Data<T>& lvalue) noexcept {
    // check for self assignment
    if (this == &lvalue)
        return *this;

    data = lvalue.data;
    return *this;
}

// Move assignment operator
template <typename T>
Data<T>& Data<T>::operator=(Data<T>&& rvalue) noexcept {
    // check for self assignment
    if (this == &rvalue)
        return *this;

    data = std::move(rvalue.data);
    return *this;
}

// Sets the data for a given asset that refers to a given feature and value
// if and only if there is no entry with the given asset and feature.
template <typename T>
void Data<T>::setData(const std::string& asset, const std::string& feature, const T& val) noexcept {
    auto got_asset = data.find(asset);
    if (got_asset == data.end()) {
        data.emplace(asset, std::unordered_map<std::string, T>({{feature, val}}));
    } else {
        auto& type_map = data.at(asset);
        auto got_type = type_map.find(feature);
        if (got_type == type_map.end()) {
            type_map.emplace(feature, val);
        }
    }
}

// Returns the value associated with the asset and feature given if and only if
// the asset and feature exist as an entry in this Data object, otherwise return
// default value of type T.
template <typename T>
T Data<T>::getData(const std::string& asset, const std::string& feature) const noexcept {
    if (!data.empty()) {
        auto got_asset = data.find(asset);
        if (got_asset != data.end()) {
            auto got_type = got_asset->second.find(feature);
            if (got_type != got_asset->second.end()) {
                return got_type->second;
            }
        }
    }
    T temp {};
    return temp;
}

// toString method allows you to turn this object into a human readable format and
// write it to the param os.
template <typename T>
void Data<T>::toString(std::ostream& os) const noexcept {
    for (auto ait = data.cbegin(); ait != data.cend(); ++ait) {
        os << "\t" << ait->first << ":\n\t\t";
        auto aits = ait->second;
        for (auto cit = aits.cbegin(); cit != aits.cend(); ++cit) {
            os << cit->first << ": " << cit->second << "\t";
        }
        os << "\n";
    }
}

/*************************************************************************************************/
/************************************* DataFrame Definition **************************************/
/*************************************************************************************************/
// Default constructor
template <typename T>
DataFrame<T>::DataFrame() noexcept {}

// Copy constructor
template <typename T>
DataFrame<T>::DataFrame(const DataFrame<T>& obj) noexcept
: formats(obj.formats), assetsToFeatures(obj.assetsToFeatures), data(obj.data) {}

// Move constructor
template <typename T>
DataFrame<T>::DataFrame(DataFrame<T>&& obj) noexcept
: formats(obj.formats), assetsToFeatures(obj.assetsToFeatures), data(obj.data) {}

// Copy assignment operator
template <typename T>
DataFrame<T>& DataFrame<T>::operator=(const DataFrame<T>& lvalue) noexcept {
    // check for self assignment
    if (this == &lvalue)
        return *this;

    data = lvalue.data;
    assetsToFeatures = lvalue.assetsToFeatures;
    formats = lvalue.formats;
    return *this;
}

// Move assignment operator
template <typename T>
DataFrame<T>& DataFrame<T>::operator=(DataFrame<T>&& rvalue) noexcept {
    // check for self assignment
    if (this == &rvalue)
        return *this;

    data = std::move(rvalue.data);
    assetsToFeatures = std::move(rvalue.assetsToFeatures);
    formats = std::move(rvalue.formats);
    return *this;
}

// Return whether or not this DataFrame object contains a given asset.
template <typename T>
bool DataFrame<T>::containsAsset(const std::string& asset) const noexcept {
    return assetsToFeatures.find(asset) != assetsToFeatures.end();
}

// Return whether or not this DataFrame object contains a given date.
template <typename T>
bool DataFrame<T>::containsDate(const bpt::ptime& date) const noexcept {
    return data.find(date) != data.end();
}

// Return a mapping of assets to their features.
template <typename T>
const std::unordered_map<std::string, std::unordered_set<std::string>>&
DataFrame<T>::getAssetAndFeatures() const noexcept {
    return assetsToFeatures;
}

// Add a new format to parse date and times by, if you had a unique format in your
// csv not already considered.
template <typename T>
void DataFrame<T>::addDateFormat(const std::string& format) noexcept {
    formats.emplace_back(std::locale::classic(), new bpt::time_input_facet(format));
}

// Adds a new formats to parse date and times by, if you had a unique format in your
// csv not already considered.
template <typename T>
void DataFrame<T>::addDateFormat(const std::vector<std::string>& format) noexcept {
    for (const std::string& f : format) {
        addDateFormat(f);
    }
}

// Insert all data from the csv into this DataFrame Object. The filename will act as the asset.
template <typename T>
void DataFrame<T>::fromCSV(const std::string& path) noexcept {
    std::string filename = path.substr(path.find_last_of("/\\") + 1);
    std::string::size_type const p(filename.find_last_of("."));
    std::string filenameWithOutExtension = filename.substr(0, p);
    fromCSV(filenameWithOutExtension, path);
}

// Insert all data from the csv into this DataFrame Object.
template <typename T>
void DataFrame<T>::fromCSV(const std::string& asset, const std::string& path) noexcept {
    if (assetsToFeatures.find(asset) != assetsToFeatures.end()) {
        std::cout << "Asset: " << asset << " already exists" << std::endl;
        return;
    }

    std::ifstream file(path.c_str()); // try to open file
    if (!file.is_open()) {
        std::cout << "Error opening file: " << path << std::endl;
        throw std::exception(); // throw exception if file could not be opened
    }

    typedef boost::tokenizer<boost::escaped_list_separator<char>> Tokenizer;
    boost::escaped_list_separator<char> sep{'\\', ',', '\"'};
    std::string row; // rows of files

    getline(file, row); // read in column header line
    row.erase(std::remove_if(row.begin(), row.end(), invalidCharLambda), row.end());
    Tokenizer ch(row, sep);
    std::vector<std::string> features(++ch.begin(), ch.end());
    assetsToFeatures.emplace(asset, std::unordered_set<std::string>(features.begin(), features.end()));

    while (getline(file, row)) {
        row.erase(std::remove_if(row.begin(), row.end(), invalidCharLambda), row.end());
        Tokenizer ch{row, sep};
        std::string dateString = *(ch.begin());
        std::vector<std::string> rowData(++ch.begin(), ch.end());

        // create ptime
        bpt::ptime date;
        for (const std::locale& format : formats) {
            std::istringstream is(dateString);
            is.imbue(format);
            is >> date;
            if (date != bpt::ptime()) break;
        }

        // check if ptime already exists
        if (data.find(date) == data.end()) { // ptime doesn't exist
            Data<T> dataObj;
            data.emplace(date, dataObj);
        }
        Data<T>& dataObj = data.at(date);
        insertData(dataObj, asset, features, rowData);
    }
}

// Returns the template representation T of the string str.
template <typename T>
T DataFrame<T>::convert(const std::string& str) const noexcept {
    std::istringstream ss(str);
    T val;
    ss >> val;
    return val;
}

// Converts each element [0, ..., N] in rowData into type T then inserts it into dataObj
// for asset and for each feature [0, ..., N] in features.
template <typename T>
void DataFrame<T>::insertData(Data<T>& dataObj, const std::string& asset,
    const std::vector<std::string>& features, const std::vector<std::string>& rowData) noexcept {
    for (size_t i = 0; i < features.size(); ++i) {
        dataObj.setData(asset, features.at(i), convert(rowData.at(i)));
    }
}


// Removes all date entries that don't have any data associated with them.
template <typename T>
void DataFrame<T>::removeEmptyDates() noexcept {
    for (auto it = data.begin(); it != data.end(); ) {
        if (it->second.empty()) {
            data.erase(it++);
        } else {
            ++it;
        }
    }
}

// Add ptime to this DataFrame object based on a time period.
template <typename T>
void DataFrame<T>::fillInGaps() noexcept {
    // TODO : NEED TO KNOW WHAT TIME GAP THEY WANT TO FILL
    // EX: Daily, Hourly, Minutely?
}

// Return the associated data for a given date, asset, and feature if and only if the
// date, asset, and feature exist, otherwise return the default empty value of type T.
template <typename T>
T DataFrame<T>::getData(const bpt::ptime& date, const std::string& asset, const std::string& feature) const noexcept {
    if (data.find(date) != data.end()) {
        return data.at(date).getData(asset, feature);
    }
    T t;
    return t;
}

// toString method allows you to turn this object into a human readable format and
// write it to the param os.
template <typename T>
void DataFrame<T>::toString(std::ostream& os) const noexcept {
    for (auto dad = data.cbegin(); dad != data.cend(); ++dad) { // dad = Date And Data
        os << getDate(dad->first) << ":\n" << dad->second << "\n";
    }
}

// Returns a string representing the date given.
template <typename T>
std::string DataFrame<T>::getDate(const bpt::ptime& date) noexcept {
    return bpt::to_iso_extended_string(date);
}

// Returns integer representing the day of the week for the given date.
// [0 = Sunday, 1 = Monday, ..., 6 = Saturday]
template <typename T>
int DataFrame<T>::getDayOfWeek(const bpt::ptime& date) noexcept {
    static const int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
    int year = date.date().year();
    int month = date.date().month();
    int day = date.date().day();
    year -= (month < 3);
    return (year + year/4 - year/100 + year/400 + t[month-1] + day) % 7;
}
#endif // DATASTORAGE_DATAFRAME_H
