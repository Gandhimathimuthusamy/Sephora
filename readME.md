# Sephora Test Solution

A Python program that orchestrates the process of executing a chain of dependent SQL scripts, in order to create the `final.products` table.

## Installation

### Requirements
* Python 2.7
* Dask 0.20.0

### Setup Instructions

1. Clone the repository

`git clone https://github.com/blessymoses/blessy_sephora_test_Python.git`

2. Navigate to the directory and create a Python 2.7 virtual environment

`cd blessy_sephora_test_Python/`

`virtualenv -p /usr/bin/python2.7 .`

`source bin/activate`

3. Install Dask library for Python parallel computing

`pip install "dask[complete]"`

4. Execute the solution script

`python create_products_table.py`

## Documentation of Solution

### Task 1 - Function that shows the dependencies between all the sql scripts, with a simple visualization. eg. showing that `tmp/item_purchase_prices.sql` depends on `raw.purchase_line_items` and `raw.purchase_item`

#### Solution:
find_dependencies() function in create_products_table.py script parses the sql scripts and lists the dependencies.
* raw tables are considered to be available in the `raw` dataset and assigned to the processed list.
*  In iteration 1, the tmp scripts which uses only the available tables in the processed list, are executed and appended to the processed list.
* The above step is looped until all the scripts are processed and a representation of `dependency level` is created as below:

```
SIMPLE VISUALIZATION OF DEPENDENCIES

ITERATION 1

raw.inventory_items - ------> tmp.inventory_items
raw.products - raw.pictures - ------> tmp.product_images
raw.variants - raw.pictures - ------> tmp.variant_images
raw.purchase_line_items - raw.purchase_items - ------> tmp.item_purchase_prices
raw.products - raw.categories - ------> tmp.product_categories

ITERATION 2

raw.variants - tmp.item_purchase_prices - tmp.variant_images - tmp.inventory_items - ------> tmp.variants
raw.products - tmp.product_images - tmp.product_categories - ------> tmp.products

ITERATION 3

tmp.products - tmp.variants - ------> final.products
```
### Task 2 - Function that runs the sql scripts in the correct order.

#### Solution:
* For each iteration in the dependency level, the sql scripts are executed using the dummy function.
* For example,
  * `tmp\inventory_items.sql`, `tmp\product_images.sql`, `tmp\variant_images.sql`, `tmp\item_purchase_prices.sql`, `tmp\product_categories.sql` are executed in iteration 1, before it is referred in `tmp\variants.sql` and `tmp\products.sql`.
  * `tmp\variants.sql` and `tmp\products.sql` are executed before `final\products.sql`.
```
ORDER OF EXECUTION OF SQL FILES

Executing tmp\inventory_items.sql........
Processed table tmp.inventory_items
Executing tmp\product_images.sql........
Processed table tmp.product_images
Executing tmp\variant_images.sql........
Processed table tmp.variant_images
Executing tmp\item_purchase_prices.sql........
Processed table tmp.item_purchase_prices
Executing tmp\product_categories.sql........
Processed table tmp.product_categories
Executing tmp\variants.sql........
Processed table tmp.variants
Executing tmp\products.sql........
Processed table tmp.products
Executing final\products.sql........
Processed table final.products
```

### Task 3 - Function that paralellizes the execution of the SQL scripts, ensuring they respect their dependencies.

#### Solution:
* **Step 1 : Directed Graph**
  * A directed graph is constructed to show the dependency between the sql scripts
  * It is a dictionary whose keys are the nodes of the graph. For each key, the corresponding value is a list containing the nodes that are 
connected by a direct arc from this node.
  * For example,
    * `tmp.variant_images  :  ['tmp.variants']` represents that `tmp\variant_images.sql` should be executed before `tmp\variants.sql`.
    * `raw.products  :  ['tmp.product_images', 'tmp.product_categories', 'tmp.products']` represents that `raw.products` should be available before `tmp\product_images.sql`, 
 `tmp\product_categories.sql` and `tmp\products.sql` are executed.
```
DIRECTED GRAPH

tmp.variant_images  :  ['tmp.variants']
raw.categories  :  ['tmp.product_categories']
raw.pictures  :  ['tmp.product_images', 'tmp.variant_images']
raw.variants  :  ['tmp.variant_images', 'tmp.variants']
raw.products  :  ['tmp.product_images', 'tmp.product_categories', 'tmp.products']
tmp.variants  :  ['final.products']
raw.purchase_items  :  ['tmp.item_purchase_prices']
tmp.inventory_items  :  ['tmp.variants']
raw.purchase_line_items  :  ['tmp.item_purchase_prices']
raw.inventory_items  :  ['tmp.inventory_items']
tmp.products  :  ['final.products']
tmp.product_categories  :  ['tmp.products']
tmp.product_images  :  ['tmp.products']
tmp.item_purchase_prices  :  ['tmp.variants']
```
* **Step 2 : Parallel Execution Paths using _Backtracking_**
  * Using the directed graph, parallel execution paths are identified.
  * _Backtracking_ technique is used to find the shortest path between first level scripts(tmp) and last level scripts(final). 
```
PARALLEL EXECUTION PATHS

['tmp.inventory_items', 'tmp.variants', 'final.products']
['tmp.product_images', 'tmp.products', 'final.products']
['tmp.variants', 'final.products']
['tmp.variant_images', 'tmp.variants', 'final.products']
['tmp.item_purchase_prices', 'tmp.variants', 'final.products']
['tmp.products', 'final.products']
['tmp.product_categories', 'tmp.products', 'final.products']
```
* **Step 3 : Backtrack Mapping**
  * A backtrack mapping structure is constructed from the parallel execution paths.
  * The value list represents the dependent scripts of the key.
  * For example,
    * `tmp\inventory_items.sql`, `tmp\variant_images.sql` and `tmp\item_purchase_prices.sql` should be executed before `tmp\variants.sql`.
    * `tmp\product_images.sql` and `tmp\product_categories.sql` should be executed before `tmp\products.sql`.
    * `tmp\variants.sql` and `tmp\products.sql` should be executed before `final\products.sql`.
 ```
 BACKTRACK MAPPING

tmp.variants  :  ['tmp.inventory_items', 'tmp.variant_images', 'tmp.item_purchase_prices']
tmp.products  :  ['tmp.product_images', 'tmp.product_categories']
final.products  :  ['tmp.variants', 'tmp.products']
 ```

* **Step 4 : Execute the scripts using _Dask_ library for parallel computing in Python**
  * A dask graph structure is constructed using the backtrack mapping, to represent the order of execution of the scripts.
  * For example, 
    * The tasks execute_sql_file0, execute_sql_file1, execute_sql_file2, execute_sql_file3 and execute_sql_file4 can execute in parallel.
    * The tasks upstream_dependency0 and upstream_dependency1 can execute in parallel.
    * upstream_dependency2 executes after the completion of upstream_dependency0 and upstream_dependency1
```
DASK GRAPH STRUCTURE

execute_sql_file0  :  (<function execute_sql_file at 0x7f34afb967d0>, 'tmp\\inventory_items.sql')
execute_sql_file1  :  (<function execute_sql_file at 0x7f34afb967d0>, 'tmp\\product_images.sql')
execute_sql_file2  :  (<function execute_sql_file at 0x7f34afb967d0>, 'tmp\\variant_images.sql')
execute_sql_file3  :  (<function execute_sql_file at 0x7f34afb967d0>, 'tmp\\item_purchase_prices.sql')
execute_sql_file4  :  (<function execute_sql_file at 0x7f34afb967d0>, 'tmp\\product_categories.sql')
upstream_dependency0  :  (<function execute_dep_sql_file at 0x7f34afb96848>, ['execute_sql_file0', 'execute_sql_file2', 'execute_sql_file3'])
upstream_dependency1  :  (<function execute_dep_sql_file at 0x7f34afb96848>, ['execute_sql_file1', 'execute_sql_file4'])
upstream_dependency2  :  (<function execute_dep_sql_file at 0x7f34afb96848>, ['upstream_dependency0', 'upstream_dependency1'])
```
   * **Parallel execution of scripts**
     * function execute_sql_file() is a dummy function for running the sql scripts.
       * Argument - file name of the sql script to be executed
     * function execute_dep_sql_file() is a dummy function for running the dependant sql script.
       * Argument - list of dependent file names
       * The function maps the file names list to the backtrack mapping and finds out the dependent script.
     * The parallel execution of scripts is given below:
       * `tmp\variants.sql` starts execution after the completion of its upstream dependencies execute_sql_file0, execute_sql_file2 and  execute_sql_file3, but before the completion of execute_sql_file4, thus achieving parallel execution, still ensuring the dependencies.
```
PARALLEL EXECUTION USING DASK

Executing tmp\inventory_items.sql........
Executing tmp\item_purchase_prices.sql........
Executing tmp\variant_images.sql........
Executing tmp\product_images.sql........
Processed table tmp.inventory_items
Processed table tmp.variant_images
Processed table tmp.item_purchase_prices
Processed table tmp.product_images
Executing tmp\product_categories.sql........
Executing tmp\variants.sql........
Processed table tmp.product_categories
Processed table tmp.variants
Executing tmp\products.sql........
Processed table tmp.products
Executing final\products.sql........
Processed table final.products
```

## Author

* **Blessy Moses** - [blessymoses](https://github.com/blessymoses)