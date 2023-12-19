#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>

#define PORT 6379
#define BUFFER_SIZE 1024
#define HASH_TABLE_SIZE 1024

pthread_mutex_t lock;
pthread_rwlock_t rwlock; 
// Add locks to realize the atomicity of the operations


// Define a structure to represent a key-value pair node
typedef struct node {
    char* key;
    void* value;
    size_t key_size;
    size_t value_size;
    struct node* next; // Pointer to the next node in the linked list (for handling collisions)
} node_t;

node_t* hashTable[HASH_TABLE_SIZE]; // Define an array of pointers to nodes as the hash table

unsigned int hash(const char* key, size_t key_size) {
    unsigned long int value = 0;
    for (size_t i = 0; i < key_size; ++i) {
        value = value * 37 + key[i];
    }
    return value % HASH_TABLE_SIZE; // Ensure the index is within the size of the hash table
}

int parseKeyValue(const char* command, char* key, size_t* key_size, char* value, size_t* value_size) {

    const char* ptr = command;
    while (*ptr && *ptr != ' ') ptr++; // saute "SET"
    if (!*ptr) return -1;
    ptr++; // saute " "

    const char* key_start = ptr;
    while (*ptr && *ptr != ' ') ptr++; // find key end
    if (!*ptr) return -1;
    *key_size = ptr - key_start;
    memcpy(key, key_start, *key_size);

    ptr++; // saute " "
    const char* value_start = ptr;
    while (*ptr) ptr++; // until end
    *value_size = ptr - value_start;
    memcpy(value, value_start, *value_size);

    return 0;
}

void setKeyValue(const char* key, size_t key_size, const void* value, size_t value_size) {
    pthread_rwlock_wrlock(&rwlock);
    unsigned int index = hash(key, key_size);
    node_t* node = hashTable[index];

    while (node != NULL && (node->key_size != key_size || strncmp(node->key, key, key_size) != 0)) {
        node = node->next;
    }

    if (node != NULL) {
        free(node->value);
        node->value = malloc(value_size);
        if (node->value == NULL) {
            perror("Failed to allocate memory for value");
            return;
        }
        memcpy(node->value, value, value_size);
        node->value_size = value_size;
        return;
    }

    node = malloc(sizeof(node_t));
    if (node == NULL) {
        perror("Failed to allocate memory for node");
        return;
    }

    node->key = malloc(key_size);
    if (node->key == NULL) {
        perror("Failed to allocate memory for key");
        free(node);
        return;
    }
    memcpy(node->key, key, key_size);
    node->key_size = key_size;

    node->value = malloc(value_size);
    if (node->value == NULL) {
        perror("Failed to allocate memory for value");
        free(node->key);
        free(node);
        return;
    }
    memcpy(node->value, value, value_size);
    node->value_size = value_size;
    node->next = hashTable[index];
    hashTable[index] = node;
    pthread_rwlock_unlock(&rwlock);
     // Stockage de stockage fiable des clefs sur le disque (=persistance)
    FILE *file = fopen("datafile.bin", "ab"); 
    if (file == NULL) {
        perror("Failed to open file");
        return;
    }

    fwrite(&key_size, sizeof(size_t), 1, file);
    fwrite(key, key_size, 1, file);
    
    fwrite(&value_size, sizeof(size_t), 1, file);
    fwrite(value, value_size, 1, file);

    fclose(file);
}

void* getKeyValue(const char* key, size_t* value_size) {
    pthread_rwlock_rdlock(&rwlock);
    size_t key_size = strlen(key);

    unsigned int index = hash(key, key_size);
    node_t* node = hashTable[index];

    while (node != NULL) {
        if (node->key_size == key_size && strncmp(node->key, key, key_size) == 0) {
            *value_size = node->value_size;
            return node->value;
        }
        node = node->next;
    }
    pthread_rwlock_unlock(&rwlock);
    return NULL;
}


int delKey(const char* key, size_t key_size) {
    unsigned int index = hash(key, key_size);
    node_t* prev = NULL;
    node_t* node = hashTable[index];

    while (node != NULL && (node->key_size != key_size || strncmp(node->key, key, key_size) != 0)) {
        prev = node;
        node = node->next;
    }

    if (node == NULL) {
        pthread_rwlock_unlock(&rwlock);
        return 0;
    }

    if (prev == NULL) {
        hashTable[index] = node->next;
    }
    else {
        prev->next = node->next;
    }

    free(node->key);
    free(node->value);
    free(node);
    
    // Remove the key from the file
    FILE *file = fopen("datafile.bin", "rb+");
    if (file == NULL) 
    {
        perror("Failed to open file for deletion");
        pthread_rwlock_unlock(&rwlock);
        return 0;  // File could not be opened
    }

    size_t current_key_size;
    size_t current_value_size;
    char current_key[BUFFER_SIZE];
    void *current_value;

    while (fread(&current_key_size, sizeof(size_t), 1, file) == 1) 
    {
        fread(current_key, current_key_size, 1, file);
        fread(&current_value_size, sizeof(size_t), 1, file);

        current_value = malloc(current_value_size);
        if (current_value == NULL) 
        {
            perror("Failed to allocate memory for current value");
            fclose(file);
            pthread_rwlock_unlock(&rwlock);
            return 0;  // Memory allocation failed
        }

        fread(current_value, current_value_size, 1, file);

        if (current_key_size == key_size && memcmp(current_key, key, key_size) == 0) 
        {
            // Key found, don't write it back to the file
            free(current_value);
            break;
        }

        // Write the key back to the file
        fwrite(&current_key_size, sizeof(size_t), 1, file);
        fwrite(current_key, current_key_size, 1, file);
        fwrite(&current_value_size, sizeof(size_t), 1, file);
        fwrite(current_value, current_value_size, 1, file);

        free(current_value);
    }

    fclose(file);
    pthread_rwlock_unlock(&rwlock);
    return 1; // Key deleted
}

int keyExists(const char* key) 
{
    pthread_rwlock_rdlock(&rwlock);
    size_t key_size = strlen(key);

    unsigned int index = hash(key, key_size);
    node_t* node = hashTable[index];

    while (node != NULL) 
    {
        if (node->key_size == key_size && strncmp(node->key, key, key_size) == 0) 
        {
            pthread_rwlock_unlock(&rwlock);
            return 1;  // Key exists
        }
        node = node->next;
    }

    pthread_rwlock_unlock(&rwlock);
    return 0;  // Key does not exist
}

void renameKey(const char* old_key, size_t old_key_size, const char* new_key, size_t new_key_size) {
    pthread_rwlock_wrlock(&rwlock);

    // chercher le key
    unsigned int old_index = hash(old_key, old_key_size);
    node_t* prev_node = NULL;
    node_t* old_node = hashTable[old_index];
    while (old_node != NULL && (old_node->key_size != old_key_size || strncmp(old_node->key, old_key, old_key_size) != 0)) {
        prev_node = old_node;
        old_node = old_node->next;
    }

    if (old_node == NULL) {
        pthread_rwlock_unlock(&rwlock);
        printf("Old key not found\n");
        return;
    }

    // creer nouvelle key
    unsigned int new_index = hash(new_key, new_key_size);
    node_t* new_node = malloc(sizeof(node_t));
    if (new_node == NULL) {
        pthread_rwlock_unlock(&rwlock);
        perror("Failed to allocate memory for new node");
        return;
    }

    new_node->key = malloc(new_key_size);
    if (new_node->key == NULL) {
        pthread_rwlock_unlock(&rwlock);
        perror("Failed to allocate memory for new key");
        free(new_node);
        return;
    }
    memcpy(new_node->key, new_key, new_key_size);
    new_node->key_size = new_key_size;
    new_node->value = old_node->value; 
    new_node->value_size = old_node->value_size;
    new_node->next = hashTable[new_index];
    hashTable[new_index] = new_node;

    // supprime node ancienne
    if (prev_node == NULL) {
        hashTable[old_index] = old_node->next;
    } else {
        prev_node->next = old_node->next;
    }
    free(old_node->key);
    free(old_node); 

    pthread_rwlock_unlock(&rwlock);
}

void processCommand(int clientSocket, char* command) {
    command[strcspn(command, "\n")] = 0; // Remove trailing newline
    char response[BUFFER_SIZE];
    size_t value_size;

    // Check the command type
    if (strncmp(command, "SET", 3) == 0) {
    pthread_mutex_lock(&lock);

    char key[BUFFER_SIZE];
    char value[BUFFER_SIZE];
    size_t key_size, value_size;

    // Parse the key-value pair from the command
    if (parseKeyValue(command, key, &key_size, value, &value_size) != 0) {
        snprintf(response, BUFFER_SIZE, "-ERROR Invalid SET command format\r\n");
    } else {
        setKeyValue(key, key_size, value, value_size);
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
    }

    pthread_mutex_unlock(&lock);
    }
    else if (strncmp(command, "GET", 3) == 0) {
    char key[BUFFER_SIZE];

    // Check if the command contains a key
    if (sscanf(command + 3, " %s", key) == 1) {
        pthread_mutex_lock(&lock);

        // Retrieve the value for the given key
        void* value = getKeyValue(key, &value_size);

        // Format the response
        if (value) {
            snprintf(response, BUFFER_SIZE, "$%.*s\r\n", (int)value_size, (char*)value);
        }
        else {
            snprintf(response, BUFFER_SIZE, "$-1\r\n");
        }
        pthread_mutex_unlock(&lock);
    }
    else {
        snprintf(response, BUFFER_SIZE, "-ERROR Invalid GET command format\r\n");
    }
}

    else if (strncmp(command, "DEL", 3) == 0) {
        pthread_mutex_lock(&lock);
        char key[BUFFER_SIZE];

        // Check if the command contains a key
        if (sscanf(command, "DEL %s", key) == 1) {
            size_t key_size = strlen(key);

            // Delete the key-value pair
            int result = delKey(key, key_size);

            // Format the response
            if (result == 1) {
                snprintf(response, BUFFER_SIZE, "+OK\r\n");
            }
            else {
                snprintf(response, BUFFER_SIZE, "-ERROR Key not found or already deleted\r\n");
            }
        }
        else {
            snprintf(response, BUFFER_SIZE, "-ERROR Invalid DEL command format\r\n");
        }
        pthread_mutex_unlock(&lock);
    }
    else if (strncmp(command, "PING", 4) == 0) {
        // PING command
        snprintf(response, BUFFER_SIZE, "+PONG\r\n");
    }
    else if (strncmp(command, "QUIT", 4) == 0) {
        // QUIT command
        snprintf(response, BUFFER_SIZE, "+OK\r\n");

        // Close the client socket and exit the thread
        close(clientSocket);
        pthread_exit(NULL);
    }
    else if (strncmp(command, "RENAME", 6) == 0) {
    char old_key[BUFFER_SIZE];
    char new_key[BUFFER_SIZE];

    // Check if the command contains old and new keys
    if (sscanf(command, "RENAME %s %s", old_key, new_key) == 2) {
        pthread_mutex_lock(&lock);
        size_t old_key_size = strlen(old_key);
        size_t new_key_size = strlen(new_key);
        
        // Rename the key
        renameKey(old_key, old_key_size, new_key, new_key_size);
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
        pthread_mutex_unlock(&lock);
    } else {
        snprintf(response, BUFFER_SIZE, "-ERROR Invalid RENAME command format\r\n");
    }
}

    else if (strncmp(command, "EXISTS", 6) == 0) 
    {
        char key[BUFFER_SIZE];

        // Check if the command contains a key
        if (sscanf(command + 6, " %s", key) == 1) 
        {
            pthread_mutex_lock(&lock);

            // Check if the key exists
            int exists = keyExists(key);
            snprintf(response, BUFFER_SIZE, ":%d\r\n", exists);  // Return 1 if key exists, 0 otherwise
            pthread_mutex_unlock(&lock);
        } 
        else 
        {
            snprintf(response, BUFFER_SIZE, "-ERROR Invalid EXISTS command format\r\n");
        }
    }
    else {
        snprintf(response, BUFFER_SIZE, "-ERROR Unknown Command\r\n"); // Unknown command
    }

    // Send the response to the client
    if (send(clientSocket, response, strlen(response), 0) < 0) {
        perror("Failed to send response");
    }
}

void* handleClient(void* arg) {
    // Extract the client socket from the argument
    int clientSocket = *(int*)arg;
    char buffer[BUFFER_SIZE];

    while (1) {
        // Reset the buffer to receive new data
        memset(buffer, 0, BUFFER_SIZE);

        // Receive data from the client
        ssize_t numBytes = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);

        // Check if the client disconnected
        if (numBytes <= 0) {
            if (numBytes == 0) {
                printf("Client disconnected\n");
                if (remove("datafile.bin") == 0) 
                {
                    printf("File deleted successfully.\n");
                }  // remove the file if the client disconnects but did not delete the keys
                else 
                {
                    perror("Error deleting file");
                }
            } else {
                perror("recv");
            }
            // Close the client socket and exit the thread
            close(clientSocket);
            return NULL;
        }
        // Process the received command
        processCommand(clientSocket, buffer);
    }
}

int main() {
    pthread_mutex_init(&lock, NULL);
    int serverSocket, *clientSocket;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);
    pthread_t thread;

    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        perror("Failed to create socket");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Failed to bind to port");
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 5) < 0) {
        perror("Failed to listen on socket");
        exit(EXIT_FAILURE);
    }

    printf("Server is listening on port %d...\n", PORT);

    while (1) {
        clientSocket = malloc(sizeof(int));
        if (clientSocket == NULL) {
            perror("Failed to allocate memory for client socket");
            continue;
        }

        *clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrLen);
        if (*clientSocket < 0) {
            perror("Failed to accept client connection");
            free(clientSocket);
            continue;
        }

        pthread_create(&thread, NULL, handleClient, (void*)clientSocket);
        pthread_detach(thread);
    }

    close(serverSocket);
    pthread_mutex_destroy(&lock);
    return 0;
}
