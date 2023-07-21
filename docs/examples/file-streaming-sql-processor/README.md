# Running the Examples


## Example 1

1. Create the input folder
  ```console
  mkdir -p out/in-example-1
  ```
2. Start the app
  ```console
  ./file-streaming-sql-processor.sh sample-application-1.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file1.json out/in-example-1 5
  ```
4. Check the `tmp/out-example-1` folder for output


## Example 2

1. Create the input folder
  ```console
  mkdir -p out/in-example-2
  ```
2. Start the app
  ```console
  ./file-streaming-sql-processor.sh sample-application-2.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file2.csv out/in-example-2 5
  ```
4. Check the `tmp/out-example-2` folder for output


## Example 3

1. Create the input folder
  ```console
  mkdir -p out/in-example-3-1
  mkdir -p out/in-example-3-2
  ```
2. Start the app
  ```console
  ./file-streaming-sql-processor.sh sample-application-3.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file1.json out/in-example-3-1 5
  ../create-file-stream.sh resources/file2.csv out/in-example-3-2 5
  ```
4. Check the `tmp/out-example-3` folder for output


## Example 4

1. Create the input folder
  ```console
  mkdir -p out/in-example-4-1
  mkdir -p out/in-example-4-2
  ```
2. Start the app
  ```console
  ./file-streaming-sql-processor.sh sample-application-4.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file1.json out/in-example-4-1 5
  ../create-file-stream.sh resources/file2.csv out/in-example-4-2 5
  ```
4. Check the `tmp/out-example-4` folder for output
