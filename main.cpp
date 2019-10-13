#include "DataFrame.h"

using namespace std;

int main() {

    string csvFilePath1 = "./Testing1.csv";
    string csvFilePath2 = "./Testing2.csv";
    string assetCSV2 = "CSV2";

    // create a DataFrame object which holds data of type double
    DataFrame<double> dataframe;

    // print out the size of the dataframe
    cout << dataframe.size() << endl;
    // print out dataframe content
    cout << dataframe << "\n" << endl;

    // add a new date format for parsing dates in a csv
    dataframe.addDateFormat("%d-%m-%Y");

    // load data from csv file path
    dataframe.fromCSV(csvFilePath1);

    // print out the size of the dataframe
    cout << dataframe.size() << endl;
    // print out dataframe content
    cout << dataframe << "\n" << endl;

    // Load data from csv file path with asset given
    dataframe.fromCSV(assetCSV2, csvFilePath2);

    // print out the size of the dataframe
    cout << dataframe.size() << endl;
    // print out dataframe content
    cout << dataframe << "\n" << endl;

    // iterate through the dataframe
    double sumOpen = 0.0;
    for (auto it = dataframe.begin(); it != dataframe.end(); ++it) {
        // it->first  : ptime object
        // it->second : Data<T> object where T is of type double
        sumOpen += it->second.getData(assetCSV2, "Open");
    }
    cout << "Sum of all Opens for asset " << assetCSV2 << ": " << sumOpen << endl;
}
