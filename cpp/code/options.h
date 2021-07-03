#ifndef OPTIONS_H
#define OPTIONS_H

// https://stackoverflow.com/questions/1008019/c-singleton-design-pattern

class Options
{
    public:

        enum class SGBD {HIVE, ORACLE};

    private:

        SGBD sgbd = SGBD::HIVE;

    public:

        Options(const Options & other) = delete;

        void operator=(const Options & other)  = delete;

        static Options & get()
        {
            static Options instance;
                                  
            return instance;
        }

        static void set_sgbd(SGBD sgbd)
        {
            Options::get().sgbd = sgbd;
        }

        static SGBD get_sgbd()
        {
            return Options::get().sgbd;
        }

    private:
        
        Options() {}
};

#endif // OPTIONS_H