#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "./interface.h"

char *input_string();
int input_int();
double input_double();
char input_char();

char *int_to_string(int value);

void print_tabs(int depth, bool end_with_dash);

#define INPUT_STRING_CHUNK_SIZE 8

char *input_string()
{
  size_t buffer_size = INPUT_STRING_CHUNK_SIZE;
  size_t index = 0;
  char *buffer = (char *)malloc(buffer_size * sizeof(char));

  // return NULL if memory allocation fails
  if (!buffer)
    memory_error_handler(__FILE__, __LINE__, __func__);

  int c;
  // read characters until EOF or newline
  while ((c = fgetc(stdin)) != EOF && c != '\n')
  {
    // check if the buffer needs to be expanded
    if (index >= buffer_size - 1)
    {
      buffer_size += INPUT_STRING_CHUNK_SIZE;
      buffer = (char *)realloc(buffer, buffer_size * sizeof(char));
      if (!buffer)
        memory_error_handler(__FILE__, __LINE__, __func__);
    }
    // store the character in the buffer
    buffer[index++] = (char)c;
  }

  // if EOF is encountered and no characters were read, free and return NULL
  if (index == 0 && c == EOF)
  {
    free(buffer);
    return NULL;
  }

  buffer[index] = '\0'; // Null-terminate the string

  // reallocate memory to match the exact string length
  buffer = (char *)realloc(buffer, (index + 1) * sizeof(char));
  if (!buffer)
    memory_error_handler(__FILE__, __LINE__, __func__);

  return buffer; // return the final string
}

int input_int()
{
  char *buffer = input_string();
  int value = atoi(buffer);
  free(buffer);
  return value;
}

double input_double()
{
  char *buffer = input_string();
  double value = atof(buffer);
  free(buffer);
  return value;
}

char input_char()
{
  char *buffer = input_string();
  char firstChar = buffer[0];
  free(buffer);
  return firstChar;
}

char *int_to_string(int value)
{
  int digit_counter = value;
  int length = value < 0 ? 3 : 2;
  while (digit_counter /= 10)
    length++;
  char *string = (char *)calloc(length, sizeof(char));
  if (!string)
    memory_error_handler(__FILE__, __LINE__, __func__);
  sprintf(string, "%d", value);
  string[length - 1] = '\0';
  return string;
}

void print_tabs(int tab_depth, bool end_with_dash)
{
  if (end_with_dash)
    while (tab_depth--)
      printf(tab_depth ? "  " : "- ");
  else
    while (tab_depth--)
      printf("  ");
}

void memory_error_handler(const char *filename, int line, const char *funcname)
{
  printf("Error: Memory allocation failed in '%s' function\n", funcname);
  printf("    at %s:%d\n", filename, line);
  exit(1);
}

cJSON *input_cjson_with_model(DBModel *model, int tab_depth)
{
  if (model == NULL)
    return NULL;

  switch (model->type)
  {
  case DBModelType_Object:
  {
    cJSON *created_object = cJSON_CreateObject();

    if (!created_object)
      memory_error_handler(__FILE__, __LINE__, __func__);

    int model_attributes_length = model->intvalue;
    DBModel *attribute_model = NULL;

    print_tabs(tab_depth, true);
    printf("<Object> %s:\n", model->key);

    for (int i = 0; i < model_attributes_length; i++)
    {
      attribute_model = model->attributes[i];

      if (attribute_model == NULL)
        continue;

      cJSON_AddItemToObject(created_object, attribute_model->key, input_cjson_with_model(attribute_model, tab_depth + 1));
    }

    return created_object;
  }

  case DBModelType_Array:
  {
    cJSON *created_array = cJSON_CreateArray();

    if (!created_array)
      memory_error_handler(__FILE__, __LINE__, __func__);

    // get array model properties
    DBModel *array_type = get_model_attr(model, DBModelAttr_ArrayTypeGetter);
    DBModel *attr_pointer = get_model_attr(model, DBModelAttr_MinLength);
    int min_length = attr_pointer ? attr_pointer->intvalue : -1;
    attr_pointer = get_model_attr(model, DBModelAttr_MaxLength);
    int max_length = attr_pointer ? attr_pointer->intvalue : -1;

    print_tabs(tab_depth, true);
    printf("<Array> %s\n", model->key);
    print_tabs(tab_depth, false);

    // if array type is not definde, return empty array
    if (array_type == NULL)
    {
      printf("(Empty array)\n");
      return created_array;
    }
    // if array max_length is 0, return empty array
    if (max_length == 0)
    {
      printf("(Empty array)\n");
      return created_array;
    }
    // check array length constraints
    if ((max_length != -1 && max_length < min_length) || min_length < -1 || max_length < -1)
    {
      printf("Error: Invalid array length constraints.\n");
      print_tabs(tab_depth, false);
      printf("(Empty array)\n");
      return created_array;
    }

    // input array length
    int needed_length = 0;
    if (min_length != -1 && min_length == max_length)
    {
      needed_length = max_length;
    }
    else
    {
      printf("array length");
      if (min_length == -1)
        min_length = 0;
      if (max_length == -1)
      {
        if (min_length != 0)
          printf(" (>=%d)", min_length);
      }
      else
        printf(" (%d~%d)", min_length, max_length);
      printf(": ");
      needed_length = input_int();
      if (needed_length < min_length)
      {
        needed_length = min_length;
        printf("Length set to %d due to minimum length requirement.\n", min_length);
      }
      else if (max_length != -1 && needed_length > max_length)
      {
        needed_length = max_length;
        printf("Length set to %d due to maximum length requirement.\n", min_length);
      }
    }

    // input array items
    char *int_to_string_buffer = NULL;
    for (int i = 0; i < needed_length; i++)
    {
      int_to_string_buffer = int_to_string(i + 1);
      array_type->key = int_to_string_buffer;

      cJSON_AddItemToArray(created_array, input_cjson_with_model(array_type, tab_depth + 1));

      free(int_to_string_buffer);
    }
    array_type->key = NULL;

    return created_array;
  }

  case DBModelType_String:
  {
    print_tabs(tab_depth, true);
    printf("<String>");
    if (model->key)
      printf(" %s", model->key);
    printf(": ");
    char *buffer = input_string();
    cJSON *created_string = cJSON_CreateString(buffer);
    free(buffer);
    return created_string;
  }

  case DBModelType_Number:
  {
    print_tabs(tab_depth, true);
    printf("<Number>");
    if (model->key)
      printf(" %s", model->key);
    printf(": ");
    return cJSON_CreateNumber(input_double());
  }

  case DBModelType_Boolean:
  {
    print_tabs(tab_depth, true);
    printf("<Boolean> ");
    if (model->key)
      printf("%s ", model->key);
    printf("(y/n): ");
    char choice = input_char();
    return cJSON_CreateBool(choice == 'y' || choice == 'Y');
  }

  case DBModelType_Null:
    return cJSON_CreateNull();

  default:
    return NULL;
  }
}

bool edit_cjson_with_model(DBModel *model, cJSON *json, int tab_depth)
{
  if (model == NULL || json == NULL)
    return false;

  switch (model->type)
  {
  case DBModelType_Object:
  {
    DBKeys *keys = get_model_keys(model);
    int keys_length = keys->length;

    // list object fields
    print_tabs(tab_depth, false);
    printf("Object fields:\n");
    DBModel *attribute_model = NULL;
    if (keys_length == 0)
    {
      print_tabs(tab_depth, false);
      printf("No fields available.\n");
      return false;
    }
    else
      for (int i = 0; i < keys_length; i++)
      {
        attribute_model = model->attributes[i];
        print_tabs(tab_depth, false);
        printf("%d - %s\n", i + 1, attribute_model->key);
      }

    // input field to edit
    print_tabs(tab_depth, false);
    printf("Select a field of <Object> ");
    if (model->key)
      printf("%s", model->key);
    printf(" (1~%d): ", keys_length);
    int attr_index = input_int() - 1;
    DBModel *selected_field_model = attr_index < 0 || attr_index >= keys_length ? NULL : model->attributes[attr_index];
    if (selected_field_model == NULL)
    {
      print_tabs(tab_depth, false);
      printf("Invalid field selection.\n");
      return false;
    }
    const char *selected_key = selected_field_model->key;
    print_tabs(tab_depth, false);
    printf("Selected key: %s\n", selected_key);

    // input field
    cJSON *selected_field_cjson = cJSON_GetObjectItem(json, selected_key);
    if (selected_field_cjson == NULL)
    {
      print_tabs(tab_depth, false);
      printf("Field does not exist in the cJSON object.\n");
      return false;
    }

    return edit_cjson_with_model(selected_field_model, selected_field_cjson, tab_depth + 1);
  }

  case DBModelType_Array:
  {
    // get array type
    DBModel *array_type = get_model_attr(model, DBModelAttr_ArrayTypeGetter);
    if (array_type == NULL)
    {
      print_tabs(tab_depth, false);
      printf("Array type not defined.\n");
      return false;
    }

    // print array length and actions
    print_tabs(tab_depth, false);
    printf("Array length: %d\n", cJSON_GetArraySize(json));
    print_tabs(tab_depth, false);
    printf("Array actions:\n");
    print_tabs(tab_depth, false);
    printf("1 - Add\n");
    print_tabs(tab_depth, false);
    printf("2 - Remove\n");
    print_tabs(tab_depth, false);
    printf("3 - Edit\n");
    print_tabs(tab_depth, false);
    printf("Select an action (1~3): ");

    // input action
    switch (input_char())
    {
    case '1': // add
    {
      cJSON *new_item = input_cjson_with_model(array_type, tab_depth + 1);
      cJSON_AddItemToArray(json, new_item);
      return true;
    }

    case '2': // remove
    {
      print_tabs(tab_depth, false);
      printf("Select an index (start from 1) to remove: ");
      cJSON_DeleteItemFromArray(json, input_int() - 1);
      return true;
    }

    case '3': // edit
    {
      print_tabs(tab_depth, false);
      printf("Select an index (start from 1) to edit: ");

      cJSON *item = cJSON_GetArrayItem(json, input_int() - 1);
      if (item == NULL)
      {
        print_tabs(tab_depth, false);
        printf("Invalid index.\n");
        return false;
      }

      print_tabs(tab_depth, false);
      printf("Current value of selected index: ");

      return edit_cjson_with_model(array_type, item, tab_depth + 1);
    }

    default:
      print_tabs(tab_depth, false);
      printf("Invalid action.\n");
      return false;
    }
  }

  case DBModelType_String:
  {
    print_tabs(tab_depth, true);
    printf("Enter a string value: ");
    char *new_value = input_string();
    cJSON_SetValuestring(json, new_value);
    free(new_value);
    return true;
  }

  case DBModelType_Number:
  {
    print_tabs(tab_depth, true);
    printf("Enter a number value: ");
    cJSON_SetNumberValue(json, input_double());
    return true;
  }

  case DBModelType_Boolean:
  {
    print_tabs(tab_depth, true);
    printf("Enter a boolean value (y/n): ");
    char new_value = input_char();
    cJSON_SetBoolValue(json, new_value == 'y' || new_value == 'Y');
    return true;
  }

  default:
    return false;
  }
}

void print_person(DBItem *item)
{
  if (item == NULL)
    return;

  cJSON *json = item->json;

  printf("----------------------------------------------------------------\n");

  printf("%-16s: %s\n", "Name", cJSON_GetObjectItem(json, "name")->valuestring);
  printf("%-16s: %s\n", "Job Title", cJSON_GetObjectItem(json, "jobTitle")->valuestring);
  printf("%-16s: %d\n", "Age", cJSON_GetObjectItem(json, "age")->valueint);
  printf("%-16s: %s\n", "Address", cJSON_GetObjectItem(json, "address")->valuestring);

  cJSON *phoneNumbers = cJSON_GetObjectItem(json, "phoneNumbers");
  int tempArraySize = cJSON_GetArraySize(phoneNumbers);
  printf("%-16s: ", tempArraySize > 1 ? "Phone Numbers" : "Phone Number");
  for (int i = 0; i < tempArraySize; i++)
  {
    printf("%s", cJSON_GetArrayItem(phoneNumbers, i)->valuestring);
    if (i != tempArraySize - 1)
      printf(", ");
  }
  printf("\n");

  cJSON *emailAddresses = cJSON_GetObjectItem(json, "emailAddresses");
  tempArraySize = cJSON_GetArraySize(emailAddresses);
  printf("%-16s: ", tempArraySize > 1 ? "Email Addresses" : "Email Address");
  for (int i = 0; i < tempArraySize; i++)
  {
    printf("%s", cJSON_GetArrayItem(emailAddresses, i)->valuestring);
    if (i != tempArraySize - 1)
      printf(", ");
  }
  printf("\n");

  printf("%-16s: %s\n", "Married", (cJSON_IsTrue(cJSON_GetObjectItem(json, "isMarried"))) ? "YES" : "NO");
  printf("%-16s: %s\n", "Employed", (cJSON_IsTrue(cJSON_GetObjectItem(json, "isEmployed"))) ? "YES" : "NO");

  printf("----------------------------------------------------------------\n");
}

void create_person(DBModel *person_model)
{
  printf("Creating a new person.\n");
  cJSON *person_json = input_cjson_with_model(person_model, 0);
  char *name = cJSON_GetObjectItem(person_json, "name")->valuestring;

  if (exists(name))
  {
    while (true)
    {
      char *buffer = NULL;
      printf("Person with this name already exists.\n");
      printf("Do you want to create this person under another name? (y/n): ");
      char choice = input_char();
      if (!(choice == 'y' || choice == 'Y'))
      {
        printf("Person has not been created.\n");
        cJSON_Delete(person_json);
        return;
      }
      printf("Enter the new name: ");
      buffer = input_string();
      cJSON_ReplaceItemInObject(person_json, "name", cJSON_CreateString(buffer));
      if (exists(buffer))
      {
        free(buffer);
        continue;
      }
      printf("Person has been successfully created.\n");
      set_item(buffer, person_json);
      free(buffer);
      break;
    }
  }
  else
  {
    printf("Person has been successfully created.\n");
    set_item(name, person_json);
  }
}

void find_person()
{
  printf("Enter the name of the person: ");
  char *name = input_string();
  DBItem *item = get_item(name);
  free(name);

  if (item == NULL)
    printf("Person not found.\n");
  else
    print_person(item);
}

void update_person(DBModel *person_model)
{
  printf("Enter the name of the person to update: ");
  char *name_buffer = input_string();
  DBItem *item = get_item(name_buffer);
  free(name_buffer);

  if (item == NULL)
  {
    printf("Person not found.\n");
    return;
  }

  // record name before edit
  name_buffer = cJSON_GetObjectItem(item->json, "name")->valuestring;
  char *before_name = (char *)calloc(strlen(name_buffer), sizeof(char));
  if (!before_name)
    memory_error_handler(__FILE__, __LINE__, __func__);
  strcpy(before_name, name_buffer);

  // edit cjson
  bool is_success = edit_cjson_with_model(person_model, item->json, 0);
  char *after_name = cJSON_GetObjectItem(item->json, "name")->valuestring;

  // if name changed, check if it exists, cancel the update
  if (strcmp(before_name, after_name) != 0)
  {
    if (exists(after_name))
    {
      // restore name
      printf("Person with this name already exists. Operation canceled.\n");
      cJSON_SetValuestring(cJSON_GetObjectItem(item->json, "name"), before_name);
      free(before_name);
      return;
    }
    rename_item(before_name, after_name);
  }

  free(before_name);
  if (is_success)
    printf("Person has been successfully updated.\n");
  else
    printf("Person has not been updated.\n");
}

void delete_person()
{
  printf("Enter the name of the person to delete: ");
  char *name = input_string();
  bool result = delete_item(name);
  free(name);

  if (result)
    printf("Person deleted successfully.\n");
  else
    printf("Person not found.\n");
}

void main_menu()
{
  // ################ Person Model ################
  // name: <string>
  // jobTitle: <string>
  // age: <number>
  // address: <string>
  // phoneNumebrs: <string[]>
  // emaildAddresses: <string[]>
  // isMarried: <boolean>
  // isEmployed: <boolean>
  DBModel *person_model = def_model(NULL, "Person", DBModelType_Object);
  def_model(person_model, "name", DBModelType_String);
  def_model(person_model, "jobTitle", DBModelType_String);
  def_model(person_model, "age", DBModelType_Number);
  def_model(person_model, "address", DBModelType_String);
  def_model(def_model(person_model, "phoneNumbers", DBModelType_Array), DBModel_ArrayTypeSymbol, DBModelType_String);
  def_model(def_model(person_model, "emailAddresses", DBModelType_Array), DBModel_ArrayTypeSymbol, DBModelType_String);
  def_model(person_model, "isMarried", DBModelType_Boolean);
  def_model(person_model, "isEmployed", DBModelType_Boolean);

  while (1)
  {
    printf("\n################ Main Menu ################\n");
    printf("Welcome to CCH's address book!!!\n");
    printf("Choose an option:\n");
    printf("C - Create a new person\n");
    printf("R - Find a person\n");
    printf("U - Update a person\n");
    printf("D - Delete a person\n");
    printf("K - List keys\n");
    printf("S - Save database\n");
    printf("X - Exit\n");
    printf("Your choice: ");

    switch (input_char())
    {
    case 'C':
    case 'c':
      create_person(person_model);
      break;

    case 'R':
    case 'r':
      find_person();
      break;

    case 'U':
    case 'u':
      update_person(person_model);
      break;

    case 'D':
    case 'd':
      delete_person();
      break;

    case 'S':
    case 's':
      save_database(DATABASE_FILENAME);
      printf("Database saved successfully.\n");
      break;

    case 'K':
    case 'k':
    {
      DBKeys *keys = get_database_keys();
      for (int i = 0; i < keys->length; i++)
        printf("%d) %s\n", i + 1, keys->keys[i]);
      free_keys(keys);
      break;
    }

    case 'X':
    case 'x':
      printf("Exiting... Good bye!\n");
      return;

    default:
      printf("Invalid choice.\n");
      break;
    }
  }
}
