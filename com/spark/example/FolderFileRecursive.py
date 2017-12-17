'''
Created on Dec 17, 2017

@author: mandar
'''
import glob
import os

'''How to run :

python FolderFileRecursive.py
'''

'''function to get file name list recursively from folder'''
def get_file_name_recursive(element_name, file_list):

    if type(element_name) == str  and os.path.isfile(element_name):
        file_list.append(element_name)
        return
    else:
        for element in glob.glob(element_name + '/*'):
            if os.path.isfile(element):
                file_list.append(element)
            else:
                get_file_name_recursive(element, file_list)
                

'''Function to get all file present in folder'''
def get_folder_file(folder_name):
    file_list = list()
    for element_name in glob.glob(folder_name):
        
        # check if element is file or folder
        if os.path.isfile(element_name):
            file_list.append(element_name)
        else:
            get_file_name_recursive(element_name, file_list)
            
    return file_list
    

'''Function to get file difference between two folder'''
def get_folder_difference(source_location_list, source_location_pattern, destination_location_list, destination_location_pattern):
    source_file_list = list()
    for source_location_name in source_location_list:
        source_file_list.append("".join(source_location_name.split(source_location_pattern)))
        
        
    destination_file_list = list()
    for destination_location_name in destination_location_list:
        destination_file_list.append("".join(destination_location_name.split(destination_location_pattern)))
    
    difference_file_list = list()
    for source_file_name in source_file_list:
        if source_file_name not in destination_file_list:
            difference_file_list.append(source_location_pattern + source_file_name)
    
    return difference_file_list
    

if __name__ == '__main__':
    folder_1_name = "/home/mandar/Versioning/dir1/*"
    folder_2_name = "/home/mandar/Versioning/dir2/*"
    folder_1_file_list = get_folder_file(folder_name=folder_1_name)
    print "Folder 1 list : "
    print folder_1_file_list
    folder_2_file_list = get_folder_file(folder_name=folder_2_name)
    print "Folder 2 list : "
    print folder_2_file_list
    
    print "Difference between Folder1 and Folder2(Folder1-Folder2)"
    difference_file_list = get_folder_difference(source_location_list=folder_1_file_list, source_location_pattern=folder_1_name[:-1] , destination_location_list=folder_2_file_list, destination_location_pattern=folder_2_name[:-1])
    print difference_file_list
    
    print "Difference between Folder2 and Folder1(Folder2-Folder1)"
    difference_file_list = get_folder_difference(source_location_list=folder_2_file_list, source_location_pattern=folder_2_name[:-1] , destination_location_list=folder_1_file_list, destination_location_pattern=folder_1_name[:-1])
    print difference_file_list
    
    
    
'''
Sample output:
mandar@mandar-HP-Pavilion-dv4-Notebook-PC:~/Versioning$ tree
.
├── dir1
│   ├── inner_dir
│   │   └── inner_file1
│   ├── inner_dir1
│   │   ├── inner_dir1_file1
│   │   └── inner_dir1_inner_1
│   │       └── inner_dir1_inner_1_file1
│   ├── inner_dir2
│   │   └── inner_file2
│   └── test1.txt
└── dir2
    ├── test1.txt
    ├── test2.txt
    └── test3.txt

6 directories, 8 files
mandar@mandar-HP-Pavilion-dv4-Notebook-PC:~/Versioning

Run Program:
python FolderFileRecursive.py

Folder 1 list : 
['/home/mandar/Versioning/dir1/test1.txt', '/home/mandar/Versioning/dir1/inner_dir/inner_file1', '/home/mandar/Versioning/dir1/inner_dir2/inner_file2', '/home/mandar/Versioning/dir1/inner_dir1/inner_dir1_file1', '/home/mandar/Versioning/dir1/inner_dir1/inner_dir1_inner_1/inner_dir1_inner_1_file1']
Folder 2 list : 
['/home/mandar/Versioning/dir2/test1.txt', '/home/mandar/Versioning/dir2/test2.txt', '/home/mandar/Versioning/dir2/test3.txt']
Difference between Folder1 and Folder2(Folder1-Folder2)
['/home/mandar/Versioning/dir1/inner_dir/inner_file1', '/home/mandar/Versioning/dir1/inner_dir2/inner_file2', '/home/mandar/
Versioning/dir1/inner_dir1/inner_dir1_file1', '/home/mandar/Versioning/dir1/inner_dir1/inner_dir1_inner_1/inner_dir1_inner_1_file1']
Difference between Folder2 and Folder1(Folder2-Folder1)
['/home/mandar/Versioning/dir2/test2.txt', '/home/mandar/Versioning/dir2/test3.txt']


'''
    
