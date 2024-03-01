project(bson C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_BSON
    "Builtin bson handling library."
    DEFAULT ON 
)

set(sources "bson_handler.c")
set(link_type)

if(HAVE_BUILTIN_EXTENSION_BSON)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_BSON OR ENABLE_BSON)
  add_library(wiredtiger_bson_handler MODULE ${sources})
  target_include_directories(
      wiredtiger_bson_handler
      PRIVATE
          ${CMAKE_SOURCE_DIR}/src/include
          ${CMAKE_BINARY_DIR}/include
          ${CMAKE_BINARY_DIR}/config
  )
  target_compile_options(
      wiredtiger_bson_handler
      PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
  )

    set_property(TARGET wiredtiger_bson_handler PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()


if(ENABLE_BSON)
    install(TARGETS wiredtiger_bson_handler
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
