#ifndef CONTAINER_H
#define CONTAINER_H

#include <vector>

namespace SQL
{
    template<typename T>
    class Container
    {
        protected:

            std::vector<T> values;

        public:

            std::size_t size() const
            {
                return values.size();
            }
            
            T & operator[](std::size_t i)
            {
                return values[i];
            }

            const T & operator[](std::size_t i) const
            {
                return values[i];
            }

            typename std::vector<T>::iterator begin()
            {
                return values.begin();
            }

            typename std::vector<T>::iterator end()
            {
                return values.end();
            }

            const typename std::vector<T>::const_iterator begin() const
            {
                return values.begin();
            }

            const typename std::vector<T>::const_iterator end() const
            {
                return values.end();
            }
    };
}

#endif // CONTAINER_H